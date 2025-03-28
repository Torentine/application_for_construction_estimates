import os
import psycopg2
from psycopg2 import sql
from parsing_object_smeta import parse_and_save_smeta
from type_opr import identify_file_type
from parsing_xml_db import parse_xml_estimate, print_estimate_structure, run_tests

# Конфигурация подключения к БД
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "qwerty123!",
    "host": "localhost",
    "port": "5432"
}


class SmetaProcessor:
    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = self.get_db_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()

    def get_db_connection(self):
        """Устанавливает соединение с PostgreSQL"""
        try:
            return psycopg2.connect(**DB_CONFIG)
        except psycopg2.Error as e:
            print(f"Ошибка подключения к базе данных: {e}")
            return None

    def get_unprocessed_local_estimates(self):
        """Получаем список необработанных локальных смет (где price IS NULL)"""
        if not self.conn:
            return []

        try:
            with self.conn.cursor() as cursor:
                query = sql.SQL("""
                    SELECT 
                        le.id,
                        le.name_local_estimate,
                        oe.name_object_estimate,
                        o.object_name,
                        le.object_estimates_id
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
            return []

    def update_estimate_price(self, estimate_id, price):
        """Обновляем цену локальной сметы"""
        if not self.conn:
            return False

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE local_estimates SET local_estimates_price = %s WHERE id = %s",
                    (price, estimate_id)
                )
                self.conn.commit()
                return True
        except psycopg2.Error as e:
            print(f"Ошибка при обновлении сметы: {e}")
            self.conn.rollback()
            return False

    def display_estimate_info(self, estimate_info):
        """Выводим информацию о смете"""
        estimate_id, local_name, object_estimate_name, object_name, _ = estimate_info
        header = f" Локальная смета ID: {estimate_id} "
        print("\n" + "=" * len(header))
        print(header)
        print("=" * len(header))
        print(f"Объект: {object_name}")
        print(f"Объектная смета: {object_estimate_name}")
        print(f"Локальная смета: {local_name}")
        print("=" * len(header))

    def process_object_smeta(self, file_path):
        """Обрабатываем объектную смету"""
        if not file_path:
            print("Объектная смета не указана, пропускаем...")
            return False

        file_type = identify_file_type(file_path)
        print(f"Тип файла объектной сметы: {file_type}")

        if file_type == "XLSX":
            return parse_and_save_smeta(file_path)
        elif file_type in ["XLS", "XML", "GGE"]:
            print(f"Обработка формата {file_type} для объектной сметы пока в разработке")
            return False
        else:
            print(f"Формат {file_type} не поддерживается для объектной сметы")
            return False

    def process_xml_estimate(self, xml_path, estimate_id):
        """Обработка XML сметы с использованием функционала из xml_start.py"""
        try:
            print("\nЗагружаем и анализируем смету...")

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

            return True, estimate_data['total_cost']
        except Exception as e:
            print(f"\nОшибка обработки: {str(e)}")
            return False, 0

    def process_local_estimate(self, estimate_info):
        """Обрабатываем одну локальную смету с поддержкой разных форматов"""
        estimate_id, local_name, object_estimate_name, object_name, _ = estimate_info

        while True:
            self.display_estimate_info(estimate_info)
            file_path = input("\nВведите путь к файлу сметы (XML/XLS/XLSX/GGE) или 'q' для выхода: ").strip()

            if file_path.lower() == 'q':
                return False

            if not self.validate_file_path(file_path):
                continue

            file_type = identify_file_type(file_path)
            print(f"Определен тип файла: {file_type}")

            try:
                processor_map = {
                    "XML": self.process_xml_estimate,
                    "XLSX": self.process_xlsx_estimate,
                    "XLS": self.process_xls_estimate,
                    "GGE": self.process_gge_estimate
                }

                if file_type in processor_map:
                    if file_type == "XML":
                        success, total_cost = processor_map[file_type](file_path, estimate_id)
                    else:
                        success, total_cost = processor_map[file_type](file_path, estimate_id)

                    if success:
                        self.update_estimate_price(estimate_id, total_cost)
                        print(f"\n{file_type} смета успешно обработана! Стоимость: {total_cost:.2f} руб.")
                        return True
                else:
                    print(f"\nФормат {file_type} не поддерживается")
                    print("Доступные форматы: XML, XLSX, XLS, GGE")

            except Exception as e:
                print(f"\nОшибка обработки: {str(e)}")

            if not self.ask_retry():
                return False

    def validate_file_path(self, file_path):
        """Проверяет валидность пути к файлу"""
        if not file_path:
            print("Ошибка: Путь к файлу не может быть пустым!")
            return False

        if not os.path.isfile(file_path):
            print("Ошибка: Файл не найден")
            return False

        return True

    def ask_retry(self):
        """Спрашивает о повторной попытке"""
        choice = input("\nПопробовать другой файл? (y/n): ").lower()
        return choice == 'y'

    def process_xlsx_estimate(self, file_path, estimate_id):
        """Заглушка для обработки XLSX формата"""
        print("\nОбработка XLSX формата для локальных смет находится в разработке")
        print("Пожалуйста, используйте XML формат или дождитесь обновления")
        return False, 0

    def process_xls_estimate(self, file_path, estimate_id):
        """Заглушка для обработки XLS формата"""
        print("\nОбработка XLS (Excel 97-2003) формата для локальных смет находится в разработке")
        print("Рекомендуется конвертировать в XLSX или XML формат")
        return False, 0

    def process_gge_estimate(self, file_path, estimate_id):
        """Заглушка для обработки GGE формата"""
        print("\nОбработка GGE формата для локальных смет находится в разработке")
        print("Пожалуйста, используйте XML формат или дождитесь обновления")
        return False, 0


def main():
    print("\n" + "=" * 60)
    print("КОМПЛЕКСНАЯ СИСТЕМА ОБРАБОТКИ СМЕТ".center(60))
    print("=" * 60)

    with SmetaProcessor() as processor:
        # Шаг 1: Обработка объектной сметы
        obj_smeta_path = input("\nВведите путь к объектной смете (Enter для пропуска): ").strip()
        if obj_smeta_path and os.path.isfile(obj_smeta_path):
            if processor.process_object_smeta(obj_smeta_path):
                print("\nОбъектная смета успешно обработана!")
            else:
                print("\nОшибка обработки объектной сметы")

        # Шаг 2: Обработка локальных смет
        print("\n" + "=" * 60)
        print("ОБРАБОТКА ЛОКАЛЬНЫХ СМЕТ".center(60))
        print("=" * 60)

        estimates = processor.get_unprocessed_local_estimates()
        if not estimates:
            print("\nВсе локальные сметы уже обработаны (нет записей с NULL в цене)")
            return

        print(f"\nНайдено {len(estimates)} необработанных локальных смет:")
        for idx, (id, name, _, _, _) in enumerate(estimates, 1):
            print(f"{idx}. {name} (ID: {id})")

        for estimate in estimates:
            if not processor.process_local_estimate(estimate):
                break

            choice = input("\nОбработать следующую смету? (y/n): ").lower()
            if choice != 'y':
                break

    print("\n" + "=" * 60)
    print("РАБОТА ПРОГРАММЫ ЗАВЕРШЕНА".center(60))
    print("=" * 60)


if __name__ == "__main__":
    main()