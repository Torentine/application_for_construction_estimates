import pandas as pd
import re
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


def parse_excel_file(file_path):
    """Чтение Excel файла (XLS или XLSX)"""
    try:
        if file_path.lower().endswith('.xlsx'):
            engine = 'openpyxl'
        elif file_path.lower().endswith('.xls'):
            engine = 'xlrd'
        else:
            raise ValueError("Поддерживаются только файлы .xls и .xlsx")

        return pd.read_excel(file_path, sheet_name=0, header=None, engine=engine)
    except Exception as e:
        print(f"Ошибка при чтении файла Excel: {e}")
        return None


def extract_estimate_info(df):
    """Извлечение точного названия объектной сметы"""
    # Ищем строку, содержащую "ОБЪЕКТНЫЙ СМЕТНЫЙ РАСЧЕТ"
    for row in range(min(50, df.shape[0])):
        for col in range(df.shape[1]):
            cell_value = str(df.iat[row, col]).strip() if pd.notna(df.iat[row, col]) else ""
            if "ОБЪЕКТНЫЙ СМЕТНЫЙ РАСЧЕТ" in cell_value.upper():
                return cell_value.strip()

    # Альтернативные варианты
    alternative_phrases = [
        "ОБЪЕКТНАЯ СМЕТА",
        "ОБЪЕКТНЫЙ РАСЧЕТ",
        "СМЕТА №",
        "ОС №"
    ]

    for phrase in alternative_phrases:
        for row in range(min(50, df.shape[0])):
            for col in range(df.shape[1]):
                cell_value = str(df.iat[row, col]).strip() if pd.notna(df.iat[row, col]) else ""
                if phrase in cell_value.upper():
                    return cell_value.strip()

    return None


def extract_cost_info(df):
    """Извлечение информации о стоимости с учетом структуры файла"""
    # Ищем строку с "Сметная стоимость"
    for row in range(min(50, df.shape[0])):
        for col in range(df.shape[1]):
            cell_value = str(df.iat[row, col]).strip() if pd.notna(df.iat[row, col]) else ""
            if "Сметная стоимость" in cell_value:
                # Проверяем 5 столбцов справа
                for offset in range(1, 6):
                    if col + offset >= df.shape[1]:
                        continue
                    cost_cell = str(df.iat[row, col + offset]).strip()
                    match = re.search(r"(\d[\d\s.,]+)\s*тыс\.?\s*руб\.?", cost_cell.replace("\xa0", " "))
                    if match:
                        try:
                            value = float(match.group(1).replace(" ", "").replace(",", "."))
                            return value * 1000
                        except ValueError:
                            continue

    return None


def extract_local_estimates(df):
    """Извлечение локальных смет с поддержкой разных формулировок"""
    local_estimates = []
    search_phrases = [
        "Локальные сметы (расчеты)",
        "ЛОКАЛЬНЫЕ СМЕТЫ",
        "Локальные сметные расчеты",
        "Локальные сметы"
    ]

    local_estimate_row = None
    for phrase in search_phrases:
        for row in range(df.shape[0]):
            cell_value = str(df.iat[row, 0]) if pd.notna(df.iat[row, 0]) else ""
            if phrase in cell_value:
                local_estimate_row = row
                break
        if local_estimate_row is not None:
            break

    if local_estimate_row is not None:
        for row in range(local_estimate_row + 1, min(local_estimate_row + 100, df.shape[0])):
            if pd.notna(df.iat[row, 1]) and pd.notna(df.iat[row, 2]):
                local_estimate_name = f"{df.iat[row, 1]} {df.iat[row, 2]}".strip()
                if local_estimate_name and local_estimate_name not in search_phrases:
                    local_estimates.append(local_estimate_name)
            elif local_estimates:  # Если уже нашли какие-то сметы и встретили пустую строку
                break

    return local_estimates


def save_to_database(object_id, estimate_name, cost_value, local_estimates):
    """Сохранение данных в базу данных"""
    conn = connect_to_db()
    if not conn:
        return False

    try:
        with conn.cursor() as cursor:
            # Вставка объектной сметы
            cursor.execute(
                """INSERT INTO object_estimates 
                   (object_id, name_object_estimate, object_estimates_price) 
                   VALUES (%s, %s, %s) RETURNING id""",
                (object_id, estimate_name, cost_value)
            )
            estimate_id = cursor.fetchone()[0]

            # Вставка локальных смет
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
    """Основная функция обработки файла"""
    try:
        df = parse_excel_file(file_path)
        if df is None:
            raise ValueError("Не удалось прочитать файл Excel")

        estimate_name = extract_estimate_info(df)
        if not estimate_name:
            raise ValueError("Не удалось извлечь название объектной сметы")

        cost_value = extract_cost_info(df)
        if cost_value is None:
            raise ValueError("Не удалось извлечь сметную стоимость")

        local_estimates = extract_local_estimates(df)
        if not local_estimates:
            print("Предупреждение: не найдено локальных смет")

        if not save_to_database(object_id, estimate_name, cost_value, local_estimates):
            raise Exception("Ошибка при сохранении в базу данных")

        return True

    except Exception as e:
        print(f"Ошибка при обработке файла {file_path}: {str(e)}")
        raise