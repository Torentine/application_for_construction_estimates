import psycopg2
import pandas as pd
from openpyxl import load_workbook

def generate_report(db_params, filename):
    try:
        # Подключаемся к БД с параметрами из main.py
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # SQL-запрос (сортировка по object_id)
        query = """
        WITH 
        ar_costs AS (
            SELECT 
                o.id AS object_id,
                o.object_name,
                SUM(le.local_estimates_price) AS total_ar_cost
            FROM 
                objects o
            JOIN object_estimates oe ON o.id = oe.object_id
            JOIN local_estimates le ON oe.id = le.object_estimates_id
            WHERE 
                le.name_local_estimate ILIKE ANY(ARRAY[
                    '%Архитектурные решения%', 
                    '%Кровля и кладка%', 
                    '%Витражи%', 
                    '%Отделочные работы%'
                ])
            GROUP BY o.id, o.object_name
        ),
        kr_costs AS (
            SELECT 
                o.id AS object_id,
                o.object_name,
                SUM(le.local_estimates_price) AS total_kr_cost
            FROM 
                objects o
            JOIN object_estimates oe ON o.id = oe.object_id
            JOIN local_estimates le ON oe.id = le.object_estimates_id
            WHERE 
                le.name_local_estimate ILIKE ANY(ARRAY[ 
                    '%Конструктивные решения%', 
                    '%Конструкции железобетонные%', 
                    '%Конструктивные и объемно-планировочные решения%', 
                    '%Конструкции металлические%', 
                    '%Железобетонные,металлические конструкции%', 
                    '%Конструкции деревянные%', 
                    '%Железобетонные конструкции%'
                ])
            GROUP BY o.id, o.object_name
        ),
        total_costs AS (
            SELECT 
                o.id AS object_id,
                o.object_name,
                SUM(oe.object_estimates_price) AS total_cost
            FROM 
                objects o
            JOIN object_estimates oe ON o.id = oe.object_id
            GROUP BY o.id, o.object_name
        )

        SELECT 
            tc.object_name,
            ROUND(COALESCE(ac.total_ar_cost, 0) / NULLIF(tc.total_cost, 0) * 100, 2) AS ar_percentage,
            ROUND(COALESCE(kc.total_kr_cost, 0) / NULLIF(tc.total_cost, 0) * 100, 2) AS kr_percentage
        FROM 
            total_costs tc
        LEFT JOIN ar_costs ac ON tc.object_id = ac.object_id
        LEFT JOIN kr_costs kc ON tc.object_id = kc.object_id
        ORDER BY tc.object_id;  -- Сортировка по object_id
        """

        cursor.execute(query)
        results = cursor.fetchall()

        # Формируем DataFrame
        df = pd.DataFrame(results, columns=["Название объекта", "% АР", "% КР"])

        # Сохраняем DataFrame в Excel
        df.to_excel(filename, index=False)

        # Открываем файл Excel с помощью openpyxl для настройки ширины столбцов
        wb = load_workbook(filename)
        sheet = wb.active

        # Подбираем ширину первого столбца под самое длинное слово
        max_length = 0
        for cell in sheet['A']:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(cell.value)
            except:
                pass
        adjusted_width = (max_length + 2)  # Добавляем немного запаса для красивого отображения
        sheet.column_dimensions['A'].width = adjusted_width

        # Сохраняем изменения в Excel
        wb.save(filename)

        print(f"\nОтчет успешно сохранен в файл: {filename}")

    except Exception as e:
        print(f"Ошибка при генерации отчета: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()


