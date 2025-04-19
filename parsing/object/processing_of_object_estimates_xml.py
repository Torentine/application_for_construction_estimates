import os
import xml.etree.ElementTree as ET
import psycopg2
from typing import Optional

from config import DB_CONFIG


def connect_to_db(
        dbname: str = None,
        user: str = None,
        password: str = None,
        host: str = None,
        port: str = None,
        **kwargs
) -> Optional[psycopg2.extensions.connection]:
    """
    Подключается к PostgreSQL с возможностью переопределения параметров

    Args:
        dbname: Название БД (переопределяет DB_CONFIG['dbname'])
        user: Пользователь (переопределяет DB_CONFIG['user'])
        password: Пароль (переопределяет DB_CONFIG['password'])
        host: Хост (переопределяет DB_CONFIG['host'])
        port: Порт (переопределяет DB_CONFIG['port'])
        **kwargs: Дополнительные параметры для psycopg2.connect()

    Returns:
        Connection object или None при ошибке
    """
    # Копируем базовую конфигурацию, чтобы не менять оригинал
    connection_params = DB_CONFIG.copy()

    # Переопределяем только те параметры, которые переданы
    if dbname is not None:
        connection_params['dbname'] = dbname
    if user is not None:
        connection_params['user'] = user
    if password is not None:
        connection_params['password'] = password
    if host is not None:
        connection_params['host'] = host
    if port is not None:
        connection_params['port'] = port

    # Добавляем дополнительные параметры (если есть)
    connection_params.update(kwargs)

    try:
        conn = psycopg2.connect(**connection_params)
        print("Успешное подключение к БД")
        return conn
    except psycopg2.Error as e:
        print(f"Ошибка подключения к PostgreSQL: {e}")
        return None


def parse_xml_file(file_path):
    """Основная функция парсинга XML файла"""
    try:
        # Извлекаем название сметы из имени файла (без расширения)
        estimate_name = os.path.splitext(os.path.basename(file_path))[0]

        # Парсим XML файл
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Ищем раздел с локальными сметами (пробуем оба возможных названия)
        chapter = None
        possible_chapter_names = [
            "Локальные сметы (расчеты)",
            "Локальные сметные расчеты"
        ]

        for chapter_name in possible_chapter_names:
            chapters = root.findall(f".//Chapter[@Caption='{chapter_name}']")
            if chapters:
                chapter = chapters[0]
                break

        if chapter is None:
            raise ValueError("Не найден раздел с локальными сметами (пробовали: " +
                             ", ".join(possible_chapter_names) + ")")

        # Извлекаем общую стоимость (из первого Summary с атрибутом Total)
        total_cost = None
        for summary in chapter.findall(".//Summary[@Total]"):
            total_cost = float(summary.get("Total"))
            break

        if total_cost is None:
            raise ValueError("Не удалось извлечь общую стоимость сметы")

        # Извлекаем локальные сметы
        local_estimates = []
        for position in chapter.findall(".//Position"):
            obosn = position.get("Obosn", "")
            caption = position.get("Caption", "")
            local_estimate_name = f"{obosn} {caption}".strip()
            if local_estimate_name:
                local_estimates.append(local_estimate_name)

        return {
            "estimate_name": estimate_name,
            "total_cost": total_cost,
            "local_estimates": local_estimates
        }

    except Exception as e:
        print(f"Ошибка при обработке XML файла: {e}")
        return None


def save_to_database(object_id, estimate_name, cost_value, local_estimates):
    """Сохранение данных в базу данных"""
    conn = connect_to_db()
    if not conn:
        return False

    try:
        # Вставка объектной сметы
        with conn.cursor() as cursor:
            cursor.execute(
                """INSERT INTO object_estimates 
                   (object_id, name_object_estimate, object_estimates_price) 
                   VALUES (%s, %s, %s) RETURNING id""",
                (object_id, estimate_name, cost_value)
            )
            estimate_id = cursor.fetchone()[0]

        # Вставка локальных смет
        with conn.cursor() as cursor:
            for estimate in local_estimates:
                cursor.execute(
                    """INSERT INTO local_estimates 
                       (object_estimates_id, name_local_estimate) 
                       VALUES (%s, %s)""",
                    (estimate_id, estimate)
                )

        conn.commit()
        return True

    except Exception as e:
        print(f"Ошибка при сохранении в базу данных: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def parse_and_save_smeta(file_path, object_id):
    """Парсит объектную смету из XML и сохраняет в базу данных"""
    # Проверка входных параметров
    if not file_path or not isinstance(file_path, (str, bytes, os.PathLike)):
        raise ValueError("Неверный путь к файлу")
    if not object_id or not isinstance(object_id, int):
        raise ValueError("Неверный ID объекта")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Файл не существует: {file_path}")
    if not file_path.lower().endswith('.xml'):
        raise ValueError("Файл должен быть в формате .xml")

    try:
        # Парсинг XML файла
        result = parse_xml_file(file_path)
        if not result:
            raise ValueError("Не удалось обработать XML файл")

        # Сохранение в базу данных
        if not save_to_database(
                object_id,
                result["estimate_name"],
                result["total_cost"],
                result["local_estimates"]
        ):
            raise Exception("Ошибка при сохранении в базу данных")

        return True

    except Exception as e:
        print(f"Ошибка при обработке файла {file_path}: {str(e)}")
        raise


def get_input_from_user():
    """Получение входных данных от пользователя"""
    print("\n" + "=" * 50)
    print(" Парсер объектных смет из XML файлов ")
    print("=" * 50 + "\n")

    while True:
        file_path = input("Введите полный путь к XML файлу сметы: ").strip()
        if not file_path:
            print("Путь к файлу не может быть пустым!")
            continue

        if not os.path.exists(file_path):
            print(f"Файл не существует: {file_path}")
            continue

        if not file_path.lower().endswith('.xml'):
            print("Файл должен иметь расширение .xml")
            continue

        break

    while True:
        object_id = input("Введите ID объекта (целое число): ").strip()
        try:
            object_id = int(object_id)
            break
        except ValueError:
            print("ID объекта должен быть целым числом!")
            continue

    return file_path, object_id


if __name__ == "__main__":
    try:
        # Получаем данные от пользователя
        file_path, object_id = get_input_from_user()

        # Обрабатываем файл
        success = parse_and_save_smeta(file_path, object_id)

        if success:
            print("\n" + "=" * 50)
            print(" ✅ Данные успешно сохранены в базу данных")
            print("=" * 50 + "\n")
        else:
            print("\n" + "=" * 50)
            print(" ❌ Не удалось сохранить данные")
            print("=" * 50 + "\n")

    except Exception as e:
        print("\n" + "=" * 50)
        print(f" ❌ Ошибка: {str(e)}")
        print("=" * 50 + "\n")
    finally:
        input("Нажмите Enter для выхода...")