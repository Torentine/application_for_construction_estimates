import pandas as pd
import re
import psycopg2
from psycopg2 import sql


def connect_to_db():
    """Подключение к базе данных PostgreSQL"""
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="qwerty123!",
            host="localhost",
            port="5432"
        )
        return conn
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None


def parse_excel_file(file_path):
    """Основная функция парсинга Excel файла"""
    try:
        # Чтение Excel-файла
        df = pd.read_excel(file_path, sheet_name=0, header=None)
        return df
    except Exception as e:
        print(f"Ошибка при чтении файла Excel: {e}")
        return None


def extract_object_info(df):
    """Извлечение информации об объекте из DataFrame"""
    # Поиск названия объекта ("на строительство")
    construction_row, construction_col = None, None
    for row in range(df.shape[0]):
        for col in range(df.shape[1]):
            if df.iat[row, col] == "на строительство":
                construction_row, construction_col = row, col
                break
        if construction_row is not None:
            break

    object_name = None
    if construction_row is not None and construction_col is not None:
        object_name = df.iat[construction_row, construction_col + 1]

    return object_name


def extract_estimate_info(df):
    """Извлечение информации о смете"""
    # Поиск названия объектной сметы ("(объектная смета)")
    estimate_row, estimate_col = None, None
    for row in range(df.shape[0]):
        for col in range(df.shape[1]):
            if df.iat[row, col] == "(объектная смета)":
                estimate_row, estimate_col = row, col
                break
        if estimate_row is not None:
            break

    estimate_name = None
    if estimate_row is not None and estimate_col is not None:
        estimate_name = df.iat[estimate_row - 1, estimate_col]

    return estimate_name


def extract_cost_info(df):
    """Извлечение информации о стоимости"""
    # Поиск сметной стоимости
    cost_row, cost_col = None, None
    for row in range(df.shape[0]):
        for col in range(df.shape[1]):
            if isinstance(df.iat[row, col], str) and "Сметная стоимость" in df.iat[row, col]:
                cost_row, cost_col = row, col
                break
        if cost_row is not None:
            break

    cost_value_number = None
    if cost_row is not None and cost_col is not None:
        cost_value = df.iat[cost_row, cost_col]
        match = re.search(r"\d+[\.,]?\d*", cost_value)
        if match:
            cost_value_cleaned = match.group(0).replace(",", ".")
            try:
                cost_value_number = float(cost_value_cleaned)
            except ValueError:
                print("Не удалось преобразовать сметную стоимость в число.")

    return cost_value_number * 1000


def extract_local_estimates(df):
    """Извлечение локальных смет"""
    local_estimates = []
    # Поиск начала локальных смет
    local_estimate_row = None
    for row in range(df.shape[0]):
        if df.iat[row, 0] == "Локальные сметы (расчеты)":
            local_estimate_row = row
            break

    if local_estimate_row is not None:
        for row in range(local_estimate_row + 1, df.shape[0]):
            if pd.notna(df.iat[row, 1]) and pd.notna(df.iat[row, 2]):
                local_estimate_name = f"{df.iat[row, 1]} {df.iat[row, 2]}"
                local_estimates.append(local_estimate_name)
            else:
                break

    return local_estimates


def save_to_database(object_name, estimate_name, cost_value, local_estimates):
    """Сохранение данных в базу данных"""
    conn = connect_to_db()
    if not conn:
        return False

    try:
        # Вставка объекта
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO objects (object_name) VALUES (%s) RETURNING id",
                (object_name,)
            )
            object_id = cursor.fetchone()[0]

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


def parse_and_save_smeta(file_path):
    """Основная функция для парсинга и сохранения сметы"""
    df = parse_excel_file(file_path)
    if df is None:
        return False

    object_name = extract_object_info(df)
    estimate_name = extract_estimate_info(df)
    cost_value = extract_cost_info(df)
    local_estimates = extract_local_estimates(df)

    if not all([object_name, estimate_name, cost_value]):
        print("Не удалось извлечь все необходимые данные из файла")
        return False

    print(f"Найден объект: {object_name}")
    print(f"Объектная смета: {estimate_name}")
    print(f"Сметная стоимость: {cost_value} тыс. руб.")
    print(f"Найдено локальных смет: {len(local_estimates)}")

    if save_to_database(object_name, estimate_name, cost_value, local_estimates):
        print("Данные успешно сохранены в базу данных")
        return True
    else:
        print("Не удалось сохранить данные в базу данных")
        return False



