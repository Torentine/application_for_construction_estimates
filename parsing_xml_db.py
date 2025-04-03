import xml.etree.ElementTree as ET
import psycopg2
from typing import Dict, List
from psycopg2 import sql

import xml.etree.ElementTree as ET
import psycopg2
from typing import Dict, List
from psycopg2 import sql


class EstimateDBHandler:
    def __init__(self, db_params):
        self.conn = psycopg2.connect(**db_params)
        self.cur = self.conn.cursor()

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def save_section(self, estimate_id: int, section_name: str) -> int:
        """Сохраняет раздел в таблицу sections и возвращает его ID"""
        query = sql.SQL("""
            INSERT INTO sections (estimate_id, name_section)
            VALUES (%s, %s)
            RETURNING id
        """)
        self.cur.execute(query, (estimate_id, section_name))
        return self.cur.fetchone()[0]

    def save_work(self, section_id: int, work_data: Dict) -> int:
        """Сохраняет работу в таблицу work и возвращает её ID"""
        query = sql.SQL("""
            INSERT INTO work (local_section_id, name_work, price, measurement_unit)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """)
        self.cur.execute(query, (
            section_id,
            work_data['caption'],
            work_data['price'],
            work_data['units']
        ))
        return self.cur.fetchone()[0]

    def save_material(self, work_id: int, material_data: Dict):
        """Сохраняет материал в таблицу materials"""
        query = sql.SQL("""
            INSERT INTO materials (work_id, name_material, price, measurement_unit)
            VALUES (%s, %s, %s, %s)
        """)
        self.cur.execute(query, (
            work_id,
            material_data['name'],
            material_data['price'],
            material_data['units']
        ))

    def update_local_estimate_price(self, estimate_id: int, total_cost: float):
        """Обновляет общую стоимость в local_estimates"""
        query = sql.SQL("""
            UPDATE local_estimates 
            SET local_estimates_price = %s
            WHERE id = %s
        """)
        self.cur.execute(query, (total_cost, estimate_id))


def parse_xml_estimate(xml_file_path: str, db_params: Dict, estimate_id: int) -> Dict:
    """
    Парсит XML смету и сохраняет данные в БД
    :param xml_file_path: путь к XML файлу
    :param db_params: параметры подключения к БД
    :param estimate_id: ID локальной сметы в БД
    :return: словарь с данными сметы
    """
    try:
        # Инициализация подключения к БД
        db_handler = EstimateDBHandler(db_params)

        # Парсинг XML
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        result = {}
        total_cost = 0.0
        current_section = None
        current_section_id = None
        current_work_id = None

        for elem in root.iter():
            if elem.tag == 'Chapter' and 'Caption' in elem.attrib:
                # Обработка раздела
                section_name = elem.get('Caption')
                current_section = section_name
                current_section_id = db_handler.save_section(estimate_id, section_name)
                result[section_name] = []

            elif elem.tag == 'Position' and 'Caption' in elem.attrib:
                position_data = {
                    'caption': elem.get('Caption'),
                    'units': elem.get('Units', ''),
                    'code': elem.get('Code', ''),
                    'code_type': None  # Определяем тип кода, но не сохраняем в БД
                }

                # Определяем тип кода (ФЕР/ТЕР/ФССЦ/ТССЦ) только для внутренней обработки
                if position_data['code'].startswith('ФЕР'):
                    position_data['code_type'] = 'ФЕР'
                elif position_data['code'].startswith('ТЕР'):
                    position_data['code_type'] = 'ТЕР'
                elif position_data['code'].startswith('ФССЦ'):
                    position_data['code_type'] = 'ФССЦ'
                elif position_data['code'].startswith('ТССЦ'):
                    position_data['code_type'] = 'ТССЦ'

                # Обработка работ (ФЕР или ТЕР)
                if position_data['code_type'] in ('ФЕР', 'ТЕР'):
                    price_base = elem.find('.//PriceBase')
                    if price_base is not None:
                        price = sum(
                            float(price_base.get(attr, '0').replace(',', '.'))
                            for attr in ['PZ', 'OZ', 'EM', 'ZM', 'MT']
                        )
                        position_data['price'] = price
                        total_cost += price

                    if current_section and current_section_id:
                        position_data['materials'] = []
                        result[current_section].append(position_data)

                        # Создаем копию данных для сохранения в БД без code_type
                        work_db_data = {
                            'caption': position_data['caption'],
                            'price': position_data['price'],
                            'units': position_data['units']
                        }
                        current_work_id = db_handler.save_work(current_section_id, work_db_data)

                # Обработка материалов (ФССЦ или ТССЦ)
                elif position_data['code_type'] in ('ФССЦ', 'ТССЦ') and current_work_id:
                    price_base = elem.find('.//PriceBase')
                    material_price = 0.0
                    if price_base is not None:
                        material_price = float(price_base.get('PZ', '0').replace(',', '.'))
                        total_cost += material_price

                    material = {
                        'name': position_data['caption'],
                        'units': position_data['units'],
                        'price': material_price
                    }

                    # Добавляем материал в текущую работу
                    if 'materials' in result[current_section][-1]:
                        result[current_section][-1]['materials'].append(material)

                    # Сохраняем материал в БД (без code_type)
                    db_handler.save_material(current_work_id, material)

        # Обновляем общую стоимость
        db_handler.update_local_estimate_price(estimate_id, total_cost)
        db_handler.conn.commit()

        result['total_cost'] = round(total_cost, 2)
        return result

    except ET.ParseError as e:
        if 'db_handler' in locals():
            db_handler.conn.rollback()
        raise Exception(f"Ошибка парсинга XML: {e}")
    except psycopg2.Error as e:
        if 'db_handler' in locals():
            db_handler.conn.rollback()
        raise Exception(f"Ошибка базы данных: {e}")
    except Exception as e:
        if 'db_handler' in locals():
            db_handler.conn.rollback()
        raise Exception(f"Произошла ошибка: {e}")


def calculate_total_cost(sections: Dict) -> float:
    """Вычисляет общую стоимость сметы на основе данных из структуры"""
    total = 0.0

    for section in sections.values():
        if isinstance(section, list):
            for work in section:
                total += work['price']
                for material in work['materials']:
                    total += material['price']

    return round(total, 2)


def run_tests(sections: Dict) -> None:
    """Запускает тесты для проверки данных сметы"""
    if not sections:
        print("Ошибка: Невозможно выполнить тесты - данные сметы не загружены")
        return

    # Собираем статистику вручную, если она не была собрана при парсинге
    if '_stats' not in sections:
        stats = {
            'total_fer': 0,
            'total_ter': 0,
            'total_fssc': 0,
            'total_tssc': 0,
            'total_chapters': 0,
            'unique_units': set(),
            'calculated_total_from_prices': 0.0
        }

        # Считаем работы и материалы
        for section_name, works in sections.items():
            if section_name in ['total_cost', '_stats']:
                continue

            stats['total_chapters'] += 1

            for work in works:
                if isinstance(work, dict):
                    if work.get('code_type') == 'ФЕР':
                        stats['total_fer'] += 1
                    elif work.get('code_type') == 'ТЕР':
                        stats['total_ter'] += 1

                    stats['calculated_total_from_prices'] += work.get('price', 0)
                    stats['unique_units'].add(work.get('units', '').strip().lower())

                    if 'materials' in work:
                        for material in work['materials']:
                            if material.get('code_type') == 'ФССЦ':
                                stats['total_fssc'] += 1
                            elif material.get('code_type') == 'ТССЦ':
                                stats['total_tssc'] += 1

                            stats['calculated_total_from_prices'] += material.get('price', 0)
                            stats['unique_units'].add(material.get('units', '').strip().lower())

        stats['total_positions'] = stats['total_fer'] + stats['total_ter'] + stats['total_fssc'] + stats['total_tssc']
        stats['unique_units_count'] = len(stats['unique_units'])

        sections['_stats'] = stats

    stats = sections['_stats']

    print("\n=== ТЕСТЫ ===")

    # Тест 1: Проверка структуры данных
    print("\nТест 1: Проверка структуры данных")
    required_keys = ['total_fer', 'total_ter', 'total_fssc', 'total_tssc', 'total_chapters',
                     'calculated_total_from_prices']
    if all(key in stats for key in required_keys):
        print("✅ Успех: Структура данных корректна")
    else:
        missing = [key for key in required_keys if key not in stats]
        print(f"❌ Ошибка: Отсутствуют ключи в статистике: {', '.join(missing)}")

    # Тест 2: Количество разделов
    print(f"\nТест 2: Количество разделов: {stats['total_chapters']}")
    actual_chapters = len([k for k in sections.keys() if k not in ['total_cost', '_stats']])
    print(f"Фактическое количество разделов: {actual_chapters}")
    if stats['total_chapters'] == actual_chapters:
        print("✅ Успех: Количество разделов совпадает")
    else:
        print("❌ Ошибка: Количество разделов не совпадает")

    # Тест 3: Проверка количества работ
    print(f"\nТест 3: Количество работ:")
    print(f"  ФЕР: {stats['total_fer']}")
    print(f"  ТЕР: {stats['total_ter']}")
    actual_fer_ter = sum(len(works) for section, works in sections.items()
                         if section not in ['total_cost', '_stats'])
    print(f"Фактическое количество работ: {actual_fer_ter}")
    if (stats['total_fer'] + stats['total_ter']) == actual_fer_ter:
        print("✅ Успех: Количество работ совпадает")
    else:
        print("❌ Ошибка: Количество работ не совпадает")

    # Тест 4: Проверка количества материалов
    print(f"\nТест 4: Количество материалов:")
    print(f"  ФССЦ: {stats['total_fssc']}")
    print(f"  ТССЦ: {stats['total_tssc']}")
    actual_fssc_tssc = sum(len(work['materials'])
                           for section, works in sections.items()
                           if section not in ['total_cost', '_stats']
                           for work in works if 'materials' in work)
    print(f"Фактическое количество материалов: {actual_fssc_tssc}")
    if (stats['total_fssc'] + stats['total_tssc']) == actual_fssc_tssc:
        print("✅ Успех: Количество материалов совпадает")
    else:
        print("❌ Ошибка: Количество материалов не совпадает")

    # Тест 5: Проверка общей стоимости
    print(f"\nТест 5: Проверка общей стоимости")
    calculated_cost = calculate_total_cost(sections)
    print(f"Расчитанная стоимость: {calculated_cost:.2f}")
    print(f"Указанная стоимость: {sections['total_cost']:.2f}")
    if abs(calculated_cost - sections['total_cost']) < 0.01:
        print("✅ Успех: Стоимости совпадают")
    else:
        print("❌ Ошибка: Стоимости не совпадают")

    # Тест 6: Проверка единиц измерения
    print(f"\nТест 6: Проверка единиц измерения")
    empty_units = sum(1 for section in sections.values()
                      if isinstance(section, list)
                      for work in section
                      if not work.get('units', '').strip())
    if empty_units == 0:
        print("✅ Успех: Пустые единицы измерения отсутствуют")
    else:
        print(f"❌ Ошибка: Найдено {empty_units} работ без указания единиц измерения")


def print_estimate_structure(sections: Dict) -> None:
    """Печатает структуру сметы"""
    print("Полная структура сметы с ценами:")
    for section_name, works in sections.items():
        if section_name in ['total_cost', '_stats']:
            continue

        print(f"\nРаздел: {section_name}")
        for i, work in enumerate(works, 1):
            code_type = work.get('code_type', '')
            print(f"  {i}. {work['caption']} [{work['units']}] ({code_type}) - {work['price']:.2f}")
            if work['materials']:
                print("    Материалы:")
                for j, material in enumerate(work['materials'], 1):
                    mat_code_type = material.get('code_type', '')
                    print(
                        f"      {j}. {material['name']} [{material['units']}] ({mat_code_type}) - {material['price']:.2f}")

    print(f"\nОбщая стоимость сметы: {sections['total_cost']:.2f}")


if __name__ == "__main__":
    # Если скрипт запускается напрямую, можно добавить обработку аргументов командной строки
    import sys

    if len(sys.argv) > 1:
        xml_file_path = sys.argv[1]
        try:
            # Пример параметров подключения к БД
            db_params = {
                'dbname': 'your_db',
                'user': 'your_user',
                'password': 'your_password',
                'host': 'localhost'
            }
            estimate_id = 1  # ID сметы в БД

            sections = parse_xml_estimate(xml_file_path, db_params, estimate_id)
            print_estimate_structure(sections)
            run_tests(sections)
        except Exception as e:
            print(f"Ошибка: {e}")
    else:
        print("Пожалуйста, укажите путь к XML файлу сметы")
