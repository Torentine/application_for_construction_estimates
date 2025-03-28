import xlrd
import pandas as pd

# Загрузка файла Excel
file_path = '02-01-01изм.1 АС.xls'  # Файл в формате .xls
sheet_name = '02-01-01изм.1_ЛС ф2'

# Открытие файла с помощью xlrd
workbook = xlrd.open_workbook(file_path)
sheet = workbook.sheet_by_name(sheet_name)

# Считывание всех данных из листа
data = [sheet.row_values(row_idx) for row_idx in range(sheet.nrows)]

# Удаление строк, которые начинаются с "Раздел"
filtered_data = [row for row in data if not (isinstance(row[0], str) and row[0].startswith("Раздел"))]

# Удаление полностью пустых строк
filtered_data = [row for row in filtered_data if not all(cell == '' or cell is None for cell in row)]

# Преобразование в DataFrame
all_data = pd.DataFrame(filtered_data)

# Поиск строки, которая начинается с цифры (например, "1")
start_row = None
for i, row in all_data.iterrows():
    if isinstance(row[0], (int, float)) and row[0] == 1:  # Проверяем, является ли первый элемент строки числом 1
        start_row = i
        break

if start_row is not None:
    # Используем предыдущую строку как заголовки столбцов
    headers = all_data.iloc[start_row - 1].tolist()

    # Чтение данных, начиная с найденной строки
    df = pd.DataFrame(all_data.iloc[start_row:].values, columns=headers)

    # Сброс индекса
    df.reset_index(drop=True, inplace=True)

    # Вывод всех строк без индексов
    pd.set_option('display.max_columns', None)  # Позволяет отображать все столбцы
    pd.set_option('display.expand_frame_repr', False)  # Не обрезает DataFrame по ширине

    print(df[:50])  # Выводим 50 строк без индексов
else:
    print("Строка, начинающаяся с цифры 1, не найдена.")