import psycopg2
from psycopg2 import sql

# Параметры подключения к БД
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "qwerty123!"
DB_HOST = "localhost"
DB_PORT = "5432"


def calculate_ar_kr_percentages():
    try:
        # Подключаемся к БД
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        # SQL запрос для расчета процентов АР и КР
        query = sql.SQL("""
        WITH 
        -- Сумма стоимостей АР по каждому объекту
        ar_costs AS (
            SELECT 
                o.id AS object_id,
                o.object_name,
                SUM(le.local_estimates_price) AS total_ar_cost
            FROM 
                objects o
            JOIN 
                object_estimates oe ON o.id = oe.object_id
            JOIN 
                local_estimates le ON oe.id = le.object_estimates_id
            WHERE 
                le.name_local_estimate ILIKE ANY(ARRAY[
                    '%Архитектурные решения%', 
                    '%Кровля и кладка%', 
                    '%Витражи%', 
                    '%Отделочные работы%'
                ])
            GROUP BY 
                o.id, o.object_name
        ),

        -- Сумма стоимостей КР по каждому объекту
        kr_costs AS (
            SELECT 
                o.id AS object_id,
                o.object_name,
                SUM(le.local_estimates_price) AS total_kr_cost
            FROM 
                objects o
            JOIN 
                object_estimates oe ON o.id = oe.object_id
            JOIN 
                local_estimates le ON oe.id = le.object_estimates_id
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
            GROUP BY 
                o.id, o.object_name
        ),

        -- Общая стоимость по каждому объекту
        total_costs AS (
            SELECT 
                o.id AS object_id,
                o.object_name,
                SUM(oe.object_estimates_price) AS total_cost
            FROM 
                objects o
            JOIN 
                object_estimates oe ON o.id = oe.object_id
            GROUP BY 
                o.id, o.object_name
        )

        -- Итоговый запрос с расчетом процентов
        SELECT 
            tc.object_name,
            ROUND(COALESCE(ac.total_ar_cost, 0) / NULLIF(tc.total_cost, 0) * 100, 2) AS ar_percentage,
            ROUND(COALESCE(kc.total_kr_cost, 0) / NULLIF(tc.total_cost, 0) * 100, 2) AS kr_percentage,
            ROUND((COALESCE(ac.total_ar_cost, 0) + COALESCE(kc.total_kr_cost, 0)) / NULLIF(tc.total_cost, 0) * 100, 2) AS total_ar_kr_percentage
        FROM 
            total_costs tc
        LEFT JOIN 
            ar_costs ac ON tc.object_id = ac.object_id
        LEFT JOIN 
            kr_costs kc ON tc.object_id = kc.object_id
        ORDER BY 
            tc.object_name;
        """)

        cursor.execute(query)
        results = cursor.fetchall()

        # Выводим результаты
        print("\nРезультаты расчета процентов АР и КР:")
        print("{:<50} {:<15} {:<15} {:<15}".format(
            "Название объекта", "% АР", "% КР", "% АР+КР"))
        print("-" * 95)

        for row in results:
            print("{:<50} {:<15.2f} {:<15.2f} {:<15.2f}".format(
                row[0], row[1], row[2], row[3]))

    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    calculate_ar_kr_percentages()