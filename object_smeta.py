import pandas as pd
import re
import psycopg2
from psycopg2 import sql

# Укажите путь к вашему Excel-файлу
file_path = '376_УКС_С_Раздел_ПД_№_11_Объектная_смета_02_01_бц.xlsx'

# Чтение Excel-файла
# Предполагаем, что данные находятся на первом листе (sheet_name=0)
df = pd.read_excel(file_path, sheet_name=0, header=None)

# Подключение к базе данных PostgreSQL
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname="postgres",  # Название вашей базы данных
            user="postgres",  # Имя пользователя
            password="qwerty123!",  # Пароль
            host="localhost",  # Хост
            port="5432"  # Порт
        )
        return conn
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None

# Функция для вставки данных в таблицу objects
def insert_object(conn, object_name):
    try:
        with conn.cursor() as cursor:
            query = sql.SQL("INSERT INTO objects (object_name) VALUES (%s) RETURNING id")
            cursor.execute(query, (object_name,))
            object_id = cursor.fetchone()[0]
            conn.commit()
            return object_id
    except Exception as e:
        print(f"Ошибка при вставке объекта: {e}")
        return None

# Функция для вставки данных в таблицу object_estimates
def insert_object_estimate(conn, object_id, estimate_name, cost_value_number):
    try:
        with conn.cursor() as cursor:
            query = sql.SQL("INSERT INTO object_estimates (object_id, name_object_estimate, object_estimates_price) VALUES (%s, %s, %s) RETURNING id")
            cursor.execute(query, (object_id, estimate_name, cost_value_number))
            estimate_id = cursor.fetchone()[0]
            conn.commit()
            return estimate_id
    except Exception as e:
        print(f"Ошибка при вставке объектной сметы: {e}")
        return None

# Функция для вставки данных в таблицу local_estimates
def insert_local_estimate(conn, estimate_id, local_estimate_name):
    try:
        with conn.cursor() as cursor:
            query = sql.SQL("INSERT INTO local_estimates (object_estimates_id, name_local_estimate) VALUES (%s, %s)")
            cursor.execute(query, (estimate_id, local_estimate_name))
            conn.commit()
    except Exception as e:
        print(f"Ошибка при вставке локальной сметы: {e}")

# Ищем строку и столбец, где находится фраза "на строительство"
construction_row, construction_col = None, None
for row in range(df.shape[0]):  # Перебираем все строки
    for col in range(df.shape[1]):  # Перебираем все столбцы
        if df.iat[row, col] == "на строительство":  # Если найдена фраза
            construction_row, construction_col = row, col
            break
    if construction_row is not None:
        break

# Если фраза "на строительство" найдена, берем следующую ячейку
if construction_row is not None and construction_col is not None:
    # Название объекта находится в следующей ячейке (правее)
    object_name = df.iat[construction_row, construction_col + 1]
    print(f"Название объекта: {object_name}")
else:
    print("Фраза 'на строительство' не найдена.")

# Ищем строку и столбец, где находится фраза "(объектная смета)"
estimate_row, estimate_col = None, None
for row in range(df.shape[0]):  # Перебираем все строки
    for col in range(df.shape[1]):  # Перебираем все столбцы
        if df.iat[row, col] == "(объектная смета)":  # Если найдена фраза
            estimate_row, estimate_col = row, col
            break
    if estimate_row is not None:
        break

# Если фраза "(объектная смета)" найдена, берем ячейку на одну строку выше
if estimate_row is not None and estimate_col is not None:
    # Название объектной сметы находится на одну строку выше
    estimate_name = df.iat[estimate_row - 1, estimate_col]
    print(f"Название объектной сметы: {estimate_name}")
else:
    print("Фраза '(объектная смета)' не найдена.")

# Ищем строку, где находится фраза "Сметная стоимость"
cost_row, cost_col = None, None
for row in range(df.shape[0]):  # Перебираем все строки
    for col in range(df.shape[1]):  # Перебираем все столбцы
        if isinstance(df.iat[row, col], str) and "Сметная стоимость" in df.iat[row, col]:  # Если найдена фраза
            cost_row, cost_col = row, col
            break
    if cost_row is not None:
        break

# Если фраза "Сметная стоимость" найдена, извлекаем значение
if cost_row is not None and cost_col is not None:
    cost_value = df.iat[cost_row, cost_col]  # Получаем значение ячейки
    # Используем регулярное выражение для извлечения числа из строки
    match = re.search(r"\d+[\.,]?\d*", cost_value)  # Ищем число с запятой или точкой
    if match:
        cost_value_cleaned = match.group(0).replace(",", ".")  # Заменяем запятую на точку
        try:
            cost_value_number = float(cost_value_cleaned)  # Преобразуем в число
            print(f"Общая сметная стоимость: {cost_value_number} тыс. руб.")
        except ValueError:
            print("Не удалось преобразовать сметную стоимость в число.")
    else:
        print("Число в строке 'Сметная стоимость' не найдено.")
else:
    print("Фраза 'Сметная стоимость' не найдена.")

# Поиск строки, где начинаются "Локальные сметные (расчеты)"
local_estimate_row = None
for row in range(df.shape[0]):  # Перебираем все строки
    if df.iat[row, 0] == "Локальные сметы (расчеты)":  # Если найдена фраза
        local_estimate_row = row
        break

# Подключение к базе данных
conn = connect_to_db()
if conn:
    try:
        # Вставляем объект в таблицу objects
        object_id = insert_object(conn, object_name)
        if object_id:
            print(f"Объект успешно добавлен с ID: {object_id}")

            # Вставляем объектную смету в таблицу object_estimates
            estimate_id = insert_object_estimate(conn, object_id, estimate_name, cost_value_number)
            if estimate_id:
                print(f"Объектная смета успешно добавлена с ID: {estimate_id}")

                # Если строка "Локальные сметные (расчеты)" найдена, обрабатываем данные
                if local_estimate_row is not None:
                    print("\nЛокальные сметные расчеты:")
                    # Перебираем строки, начиная со следующей после "Локальные сметные (расчеты)"
                    for row in range(local_estimate_row + 1, df.shape[0]):
                        # Проверяем, что строка содержит данные (не пустая)
                        if pd.notna(df.iat[row, 1]) and pd.notna(df.iat[row, 2]):
                            # Формируем название локальной сметы: Номера сметных расчетов + Наименование работ и затрат
                            local_estimate_name = f"{df.iat[row, 1]} {df.iat[row, 2]}"
                            # Вставляем локальную смету в таблицу local_estimates
                            insert_local_estimate(conn, estimate_id, local_estimate_name)
                            print(f"Локальная смета добавлена: {local_estimate_name}")
                        else:
                            # Если данные закончились, прерываем цикл
                            break
                else:
                    print("Строка 'Локальные сметы (расчеты)' не найдена.")
    finally:
        # Закрываем соединение с базой данных
        conn.close()
else:
    print("Не удалось подключиться к базе данных.")