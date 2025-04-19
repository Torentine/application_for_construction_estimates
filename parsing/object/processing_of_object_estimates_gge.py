import os
import xml.etree.ElementTree as ET
import psycopg2
from typing import Dict, List, Optional, Union

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

def parse_gge_file(file_path: Union[str, bytes, os.PathLike]) -> Optional[Dict[str, Union[str, float, List[str]]]]:
    """Основная функция парсинга GGE файла"""
    try:
        # Извлекаем название сметы из имени файла (без расширения .gge)
        estimate_name = os.path.splitext(os.path.basename(file_path))[0]

        # Парсим XML файл
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Извлекаем общую стоимость из тега <Total> внутри <Summary>
        summary = root.find(".//Object/Summary")
        if summary is None:
            raise ValueError("Не найден раздел с общей стоимостью (Summary)")

        total_element = summary.find("Total")
        if total_element is None or not total_element.text:
            raise ValueError("Не удалось извлечь общую стоимость сметы")

        try:
            total_cost = float(total_element.text)
        except ValueError:
            raise ValueError("Некорректный формат общей стоимости")

        # Извлекаем локальные сметы
        local_estimates = []
        for estimate in root.findall(".//LocalEstimate"):
            reason = estimate.find("Reason")
            name = estimate.find("Name")

            if reason is not None and name is not None and reason.text and name.text:
                local_estimate_name = f"{reason.text.strip()} {name.text.strip()}"
                local_estimates.append(local_estimate_name)

        return {
            "estimate_name": estimate_name,
            "total_cost": total_cost,
            "local_estimates": local_estimates
        }

    except ET.ParseError as e:
        print(f"Ошибка парсинга XML: {e}")
        return None
    except Exception as e:
        print(f"Ошибка при обработке GGE файла: {e}")
        return None


def save_to_database(object_id: int, estimate_name: str, cost_value: float,
                     local_estimates: List[str]) -> bool:
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
        if conn:
            conn.close()


def parse_and_save_smeta(file_path: Union[str, bytes, os.PathLike], object_id: int) -> bool:
    """Парсит объектную смету из GGE и сохраняет в базу данных"""
    try:
        # Проверка входных параметров
        if not isinstance(file_path, (str, bytes, os.PathLike)):
            raise TypeError("Некорректный тип пути к файлу")

        file_path = os.path.abspath(os.path.normpath(file_path))

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Файл не существует: {file_path}")

        if not file_path.lower().endswith('.gge'):
            raise ValueError("Файл должен иметь расширение .gge")

        # Парсинг GGE файла
        result = parse_gge_file(file_path)
        if not result:
            raise ValueError("Не удалось обработать GGE файл")

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


def main():
    """Основная функция для тестирования"""
    print("\n" + "=" * 50)
    print(" Парсер объектных смет из GGE файлов ")
    print("=" * 50 + "\n")

    while True:
        file_path = input("Введите полный путь к GGE файлу сметы: ").strip()
        if not file_path:
            print("Путь к файлу не может быть пустым!")
            continue

        if not os.path.exists(file_path):
            print(f"Файл не существует: {file_path}")
            continue

        if not file_path.lower().endswith('.gge'):
            print("Файл должен иметь расширение .gge")
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

    try:
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


if __name__ == "__main__":
    main()