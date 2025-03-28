import psycopg2
from psycopg2 import sql
from parsing_xml_db import parse_xml_estimate, print_estimate_structure, run_tests

# Конфигурация подключения к БД
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "qwerty123!",
    "host": "localhost",
    "port": "5432"
}


def get_db_connection():
    """Устанавливает соединение с PostgreSQL"""
    try:
        return psycopg2.connect(**DB_CONFIG)
    except psycopg2.Error as e:
        print(f"Ошибка подключения к базе данных: {e}")
        raise


def get_unprocessed_local_estimates():
    """Получаем список необработанных локальных смет с информацией об объекте и объектной смете"""
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            query = sql.SQL("""
                SELECT 
                    le.id,
                    le.name_local_estimate,
                    oe.name_object_estimate,
                    o.object_name
                FROM local_estimates le
                JOIN object_estimates oe ON le.object_estimates_id = oe.id
                JOIN objects o ON oe.object_id = o.id
                WHERE le.local_estimates_price IS NULL
                ORDER BY le.id
            """)
            cursor.execute(query)
            return cursor.fetchall()
    except psycopg2.Error as e:
        print(f"Ошибка при получении списка смет: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def display_estimate_info(estimate_id, local_name, object_estimate_name, object_name):
    """Выводим информацию о смете"""
    header = f" Локальная смета ID: {estimate_id} "
    print("\n" + "=" * len(header))
    print(header)
    print("=" * len(header))
    print(f"Объект: {object_name}")
    print(f"Объектная смета: {object_estimate_name}")
    print(f"Локальная смета: {local_name}")
    print("=" * len(header))


def process_single_estimate(estimate_id, local_name, object_estimate_name, object_name):
    """Обрабатываем одну локальную смету"""
    while True:
        display_estimate_info(estimate_id, local_name, object_estimate_name, object_name)
        xml_path = input("\nВведите полный путь к XML файлу сметы (или 'q' для выхода): ").strip()

        if xml_path.lower() == 'q':
            return False

        if not xml_path:
            print("Ошибка: Путь к файлу не может быть пустым!")
            continue

        try:
            print("\nЗагружаем и анализируем смету...")

            # Парсим XML и сохраняем в БД
            estimate_data = parse_xml_estimate(
                xml_file_path=xml_path,
                db_params=DB_CONFIG,
                estimate_id=estimate_id
            )

            # Выводим результаты
            print("\n" + "=" * 50)
            print("РЕЗУЛЬТАТЫ ОБРАБОТКИ".center(50))
            print("=" * 50)

            print_estimate_structure(estimate_data)
            run_tests(estimate_data)

            print("\n" + "=" * 50)
            print(f"Локальная смета успешно обработана!".center(50))
            print(f"Общая стоимость: {estimate_data['total_cost']:.2f} руб.".center(50))
            print("=" * 50)

            return True

        except FileNotFoundError:
            print("\nОшибка: Файл не найден. Проверьте путь и попробуйте еще раз.")
        except psycopg2.Error as e:
            print(f"\nОшибка базы данных: {e.pgerror}")
        except Exception as e:
            print(f"\nОшибка обработки: {str(e)}")

        choice = input("\nПопробовать снова? (y/n): ").lower()
        if choice != 'y':
            return False


def main():
    print("\n" + "=" * 50)
    print("СИСТЕМА АНАЛИЗА ЛОКАЛЬНЫХ СМЕТ".center(50))
    print("=" * 50)

    try:
        estimates = get_unprocessed_local_estimates()

        if not estimates:
            print("\nВсе локальные сметы уже обработаны.")
            return

        print(f"\nНайдено {len(estimates)} необработанных смет:")
        for idx, (id, name, _, _) in enumerate(estimates, 1):
            print(f"{idx}. {name} (ID: {id})")

        for estimate in estimates:
            estimate_id, local_name, object_estimate_name, object_name = estimate

            if not process_single_estimate(estimate_id, local_name, object_estimate_name, object_name):
                break

            choice = input("\nОбработать следующую смету? (y/n): ").lower()
            if choice != 'y':
                break

    except psycopg2.Error as e:
        print(f"\nКритическая ошибка базы данных: {e.pgerror}")
    except Exception as e:
        print(f"\nНеожиданная ошибка: {str(e)}")
    finally:
        print("\n" + "=" * 50)
        print("Работа программы завершена".center(50))
        print("=" * 50)


if __name__ == "__main__":
    main()