import xml.etree.ElementTree as ET


def calculate_total_cost(sections):
    """Вычисляет общую стоимость сметы"""
    total = 0.0

    for section in sections.values():
        for work in section:
            total += work['price']  # Добавляем стоимость работы

            # Добавляем стоимость всех материалов
            for material in work['materials']:
                total += material['price']

    return round(total, 2)


def extract_sections_works_units_prices_and_materials(xml_file):
    """
    Извлекает из XML-файла сметы:
    - названия разделов
    - работы (с кодом ФЕР) с ценами
    - материалы (с кодом ФССЦ) с ценами
    - все цены в тыс. рублей
    - общую стоимость сметы
    """
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        result = {}
        current_section = None
        last_work = None

        for elem in root.iter():
            if elem.tag == 'Chapter' and 'Caption' in elem.attrib:
                section_name = elem.get('Caption')
                if section_name not in result:
                    result[section_name] = []
                current_section = section_name

            elif elem.tag == 'Position' and 'Caption' in elem.attrib:
                position_data = {
                    'caption': elem.get('Caption'),
                    'units': elem.get('Units', ''),
                    'code': elem.get('Code', '')
                }

                # Обработка работ (ФЕР)
                if position_data['code'].startswith('ФЕР'):
                    price_base = elem.find('.//PriceBase')
                    if price_base is not None:
                        price = sum(
                            float(price_base.get(attr, '0').replace(',', '.'))
                            for attr in ['PZ', 'OZ', 'EM', 'ZM', 'MT']
                        )
                        position_data['price'] = price

                    if current_section:
                        position_data['materials'] = []
                        result[current_section].append(position_data)
                        last_work = position_data

                # Обработка материалов (ФССЦ)
                elif position_data['code'].startswith('ФССЦ'):
                    if last_work:
                        price_base = elem.find('.//PriceBase')
                        material_price = 0.0
                        if price_base is not None:
                            material_price = float(price_base.get('PZ', '0').replace(',', '.'))

                        material = {
                            'name': position_data['caption'],
                            'units': position_data['units'],
                            'price': material_price
                        }
                        last_work['materials'].append(material)

        # Добавляем общую стоимость в результат
        result['total_cost'] = calculate_total_cost(result)
        return result

    except ET.ParseError as e:
        print(f"Ошибка парсинга XML: {e}")
        return {}
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        return {}


if __name__ == "__main__":
    xml_file_path = "376-УКС_С Раздел ПД № 11 02-01-01 КР.xml"
    sections = extract_sections_works_units_prices_and_materials(xml_file_path)

    print("Полная структура сметы с ценами:")
    for section_name, works in sections.items():
        if section_name == 'total_cost':
            continue

        print(f"\nРаздел: {section_name}")
        for i, work in enumerate(works, 1):
            print(f"  {i}. {work['caption']} [{work['units']}] - {work['price']:.2f} тыс. руб.")
            if work['materials']:
                print("    Материалы:")
                for j, material in enumerate(work['materials'], 1):
                    print(f"      {j}. {material['name']} [{material['units']}] - {material['price']:.2f} тыс. руб.")

    print(f"\nОбщая стоимость сметы: {sections['total_cost']:.2f} тыс. руб.")