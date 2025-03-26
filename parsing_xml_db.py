import xml.etree.ElementTree as ET


def calculate_price(price_base):
    """Вычисляет общую цену из элементов PriceBase"""
    try:
        total = 0.0
        for attr in ['PZ', 'OZ', 'EM', 'ZM', 'MT']:
            value = price_base.get(attr)
            if value:
                # Заменяем запятые на точки для корректного преобразования в float
                total += float(value.replace(',', '.'))
        return round(total, 2)
    except Exception as e:
        print(f"Ошибка при расчете цены: {e}")
        return 0.0


def get_material_price(price_base):
    """Извлекает цену материала (значение PZ)"""
    try:
        if price_base is not None:
            pz_value = price_base.get('PZ')
            if pz_value:
                return float(pz_value.replace(',', '.'))
        return 0.0
    except Exception as e:
        print(f"Ошибка при извлечении цены материала: {e}")
        return 0.0


def get_quantity(element):
    """Извлекает количество из элемента Quantity"""
    try:
        quantity = element.find('Quantity')
        if quantity is not None:
            result = quantity.get('Result')
            if result:
                return float(result.replace(',', '.'))
        return 1.0  # Если количество не указано, считаем как 1 единицу
    except Exception as e:
        print(f"Ошибка при извлечении количества: {e}")
        return 1.0


def process_estimate(xml_file_path):
    """
    Обрабатывает XML-смету, извлекая:
    - разделы
    - работы (ФЕР) с их характеристиками
    - материалы (ФССЦ) с единицами измерения и ценами
    - общую стоимость сметы
    """
    try:
        tree = ET.parse(xml_file_path)
        root = tree.getroot()

        result = {
            'sections': {},
            'total_cost': 0.0,
            'works_cost': 0.0,
            'materials_cost': 0.0
        }

        # Находим все элементы Chapter (разделы сметы)
        chapters = root.findall(".//Chapter")

        for chapter in chapters:
            chapter_caption = chapter.get('Caption')
            if not chapter_caption:
                continue

            # Инициализируем списки для текущего раздела
            section_data = {
                'works': [],
                'section_cost': 0.0,
                'works_cost': 0.0,
                'materials_cost': 0.0
            }
            temp_works = []  # Временный список работ без материалов
            temp_materials = []  # Временный список материалов

            # Обрабатываем все элементы внутри раздела
            for element in chapter:
                if element.tag == 'Position':
                    caption = element.get('Caption')
                    code = element.get('Code')
                    units = element.get('Units')
                    quantity = get_quantity(element)

                    # Если это работа (код начинается с ФЕР)
                    if code and code.startswith('ФЕР'):
                        # Если есть накопленные материалы - добавляем их к работам
                        if temp_materials:
                            for work in temp_works:
                                # Создаем копию материалов для каждой работы
                                work_with_materials = {
                                    'name': work['name'],
                                    'code': work['code'],
                                    'units': work['units'],
                                    'price': work['price'],
                                    'quantity': work['quantity'],
                                    'total_cost': work['price'] * work['quantity'],
                                    'materials': temp_materials.copy(),
                                    'materials_cost': sum(m['price'] * m['quantity'] for m in temp_materials)
                                }
                                section_data['works'].append(work_with_materials)
                                section_data['works_cost'] += work_with_materials['total_cost']
                                section_data['materials_cost'] += work_with_materials['materials_cost']
                                section_data['section_cost'] += (work_with_materials['total_cost'] +
                                                                 work_with_materials['materials_cost'])

                            # Очищаем временные списки
                            temp_works = []
                            temp_materials = []

                        # Добавляем новую работу во временный список
                        price = calculate_price(element.find('PriceBase'))
                        temp_works.append({
                            'name': caption,
                            'code': code,
                            'units': units,
                            'price': price,
                            'quantity': quantity
                        })

                    # Если это материал (код начинается с ФССЦ)
                    elif code and (code.startswith('ФССЦ') or code.startswith('ФССЦпг')):
                        # Извлекаем единицы измерения, цену и количество материала
                        material_units = element.get('Units', '')
                        material_price = get_material_price(element.find('PriceBase'))
                        material_quantity = get_quantity(element)

                        # Добавляем материал с дополнительной информацией
                        temp_materials.append({
                            'name': caption,
                            'units': material_units,
                            'price': material_price,
                            'quantity': material_quantity,
                            'total_cost': material_price * material_quantity
                        })

            # Обрабатываем оставшиеся работы после завершения раздела
            for work in temp_works:
                work_with_materials = {
                    'name': work['name'],
                    'code': work['code'],
                    'units': work['units'],
                    'price': work['price'],
                    'quantity': work['quantity'],
                    'total_cost': work['price'] * work['quantity'],
                    'materials': temp_materials.copy(),
                    'materials_cost': sum(m['price'] * m['quantity'] for m in temp_materials)
                }
                section_data['works'].append(work_with_materials)
                section_data['works_cost'] += work_with_materials['total_cost']
                section_data['materials_cost'] += work_with_materials['materials_cost']
                section_data['section_cost'] += (work_with_materials['total_cost'] +
                                                 work_with_materials['materials_cost'])

            if section_data['works']:
                result['sections'][chapter_caption] = section_data
                result['works_cost'] += section_data['works_cost']
                result['materials_cost'] += section_data['materials_cost']
                result['total_cost'] += section_data['section_cost']

        return result

    except ET.ParseError as e:
        print(f"Ошибка парсинга XML: {e}")
        return {}
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        return {}


# Пример использования
if __name__ == "__main__":
    xml_path = "376-УКС_С Раздел ПД № 11 02-01-01 КР.xml"  # Укажите путь к вашему XML-файлу
    estimate_data = process_estimate(xml_path)

    print("Полная структура сметы с расчетом стоимости:")
    for section_name, section in estimate_data['sections'].items():
        print(f"\nРаздел: {section_name}")
        print("=" * 50)
        print(f"Общая стоимость раздела: {section['section_cost']:.2f} руб.")
        print(f"Стоимость работ: {section['works_cost']:.2f} руб.")
        print(f"Стоимость материалов: {section['materials_cost']:.2f} руб.")

        for i, work in enumerate(section['works'], 1):
            print(f"\n{i}. {work['name']}")
            print(f"   Код: {work['code']}")
            print(f"   Единицы измерения: {work['units']}")
            print(f"   Цена за единицу: {work['price']:.2f} руб.")
            print(f"   Количество: {work['quantity']}")
            print(f"   Стоимость работы: {work['total_cost']:.2f} руб.")

            if work['materials']:
                print("   Материалы:")
                for j, material in enumerate(work['materials'], 1):
                    print(f"     {j}. {material['name']}")
                    print(f"         Единицы измерения: {material['units']}")
                    print(f"         Цена за единицу: {material['price']:.2f} руб.")
                    print(f"         Количество: {material['quantity']}")
                    print(f"         Стоимость: {material['total_cost']:.2f} руб.")
                print(f"   Общая стоимость материалов для работы: {work['materials_cost']:.2f} руб.")
            else:
                print("   Материалы: нет")

    print("\n" + "=" * 50)
    print("ИТОГО ПО СМЕТЕ:")
    print(f"Общая стоимость работ: {estimate_data['works_cost']:.2f} руб.")
    print(f"Общая стоимость материалов: {estimate_data['materials_cost']:.2f} руб.")
    print(f"ОБЩАЯ СТОИМОСТЬ СМЕТЫ: {estimate_data['total_cost']:.2f} руб.")