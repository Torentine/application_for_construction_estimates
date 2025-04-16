import os

import psycopg2
from psycopg2 import sql
from config import DB_CONFIG
from parsing.parsing_object_smeta import parse_and_save_smeta
from type_opr import identify_file_type
from parsing.parsing_xml_db import parse_xml_estimate

class SmetaProcessor:
    def __init__(self):
        self.conn = None

    def __enter__(self):
        self.conn = self.get_db_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
        if exc_val:
            print(f"Ошибка в SmetaProcessor: {exc_val}")

    def get_db_connection(self):
        try:
            return psycopg2.connect(**DB_CONFIG)
        except psycopg2.Error as e:
            raise Exception(f"Ошибка подключения к базе данных: {e}")

    def get_unprocessed_local_estimates(self):
        """Возвращает список необработанных локальных смет"""
        if not self.conn:
            raise Exception("Нет подключения к базе данных")

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
            raise Exception(f"Ошибка при получении списка смет: {e}")

    def update_estimate_price(self, estimate_id, price):
        if not self.conn:
            raise Exception("Нет подключения к базе данных")

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE local_estimates SET local_estimates_price = %s WHERE id = %s",
                    (price, estimate_id)
                )
                self.conn.commit()
                return True
        except psycopg2.Error as e:
            self.conn.rollback()
            raise Exception(f"Ошибка при обновлении сметы: {e}")

    def process_object_smeta(self, file_path, object_id):
        """Обработка объектной сметы с улучшенной обработкой ошибок"""
        try:
            # Явная проверка типа пути
            if not isinstance(file_path, (str, bytes, os.PathLike)):
                raise TypeError("Некорректный тип пути к файлу")

            # Нормализация пути
            file_path = os.path.abspath(os.path.normpath(file_path))

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Файл не существует: {file_path}")

            file_type = identify_file_type(file_path)
            print(f"Тип файла объектной сметы: {file_type}")

            if file_type == "XLSX":
                return parse_and_save_smeta(file_path, object_id)
            else:
                raise ValueError(f"Неподдерживаемый формат файла: {file_type}")

        except Exception as e:
            print(f"Ошибка в process_object_smeta: {str(e)}")
            raise

    def process_xml_estimate(self, xml_path, estimate_id):
        try:
            estimate_data = parse_xml_estimate(
                xml_file_path=xml_path,
                db_params=DB_CONFIG,
                estimate_id=estimate_id
            )
            return True, estimate_data['total_cost']
        except Exception as e:
            raise Exception(f"Ошибка обработки XML: {str(e)}")

    def delete_empty_object_estimates(self, object_id):
        """Удаляет пустые объектные сметы для указанного объекта"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM object_estimates
                    WHERE object_id = %s AND id NOT IN (
                        SELECT DISTINCT object_estimates_id FROM local_estimates
                    )
                """, (object_id,))
                return cursor.rowcount
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Ошибка при удалении пустых объектных смет: {e}")

    def delete_object_if_empty(self, object_id):
        """Удаляет объект, если у него нет смет"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM objects
                    WHERE id = %s AND id NOT IN (
                        SELECT DISTINCT object_id FROM object_estimates
                    )
                """, (object_id,))
                return cursor.rowcount
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Ошибка при удалении пустого объекта: {e}")

    def get_full_hierarchy(self):
        """Получение полной иерархии объектов, смет и разделов"""
        try:
            with self.conn.cursor() as cursor:
                hierarchy = {'objects': []}

                # Получаем все объекты
                cursor.execute("SELECT id, object_name FROM objects ORDER BY object_name")
                objects = cursor.fetchall()

                for obj_id, obj_name in objects:
                    obj_data = {
                        'id': obj_id,
                        'name': obj_name,
                        'object_estimates': []
                    }

                    # Получаем объектные сметы для объекта
                    cursor.execute(
                        "SELECT id, name_object_estimate, object_estimates_price "
                        "FROM object_estimates WHERE object_id = %s ORDER BY name_object_estimate",
                        (obj_id,)
                    )
                    obj_estimates = cursor.fetchall()

                    for oe_id, oe_name, oe_price in obj_estimates:
                        oe_data = {
                            'id': oe_id,
                            'name': oe_name,
                            'price': oe_price,
                            'local_estimates': []
                        }

                        # Получаем локальные сметы для объектной сметы
                        cursor.execute(
                            "SELECT id, name_local_estimate, local_estimates_price "
                            "FROM local_estimates WHERE object_estimates_id = %s "
                            "ORDER BY name_local_estimate",
                            (oe_id,)
                        )
                        local_estimates = cursor.fetchall()

                        for le_id, le_name, le_price in local_estimates:
                            oe_data['local_estimates'].append({
                                'id': le_id,
                                'name': le_name,
                                'price': le_price
                            })

                        obj_data['object_estimates'].append(oe_data)

                    hierarchy['objects'].append(obj_data)

                return hierarchy

        except Exception as e:
            raise Exception(f"Ошибка при получении иерархии: {str(e)}")
