# sorting_2.py
import datetime
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus


def process_work_data(engine, object_ids=None):
    """
    Обрабатывает данные по работам:
    - Группирует по полным кодам
    - Вычисляет удельную стоимость
    - Подсчитывает количество разных смет и общее количество вхождений
    """
    ids_filter = ''
    if object_ids:
        ids_filter = f"WHERE obj.id IN ({','.join(map(str, set(object_ids)))})"

    query = f"""
    SELECT 
        w.code AS "Код работы",
        w.name_work AS "Наименование работы",
        w.measurement_unit AS "Единица измерения",
        le.object_estimates_id,
        w.price AS "Стоимость работы",
        oe.object_estimates_price AS "Стоимость объектной сметы"
    FROM work w
    JOIN sections s ON w.local_section_id = s.id
    JOIN local_estimates le ON s.estimate_id = le.id
    JOIN object_estimates oe ON le.object_estimates_id = oe.id
    JOIN objects obj ON oe.object_id = obj.id
    {ids_filter}
    """
    df = pd.read_sql(query, engine)
    df['Удельная стоимость, %'] = df['Стоимость работы'] / df['Стоимость объектной сметы']

    grouped = df.groupby('Код работы').agg({
        'Наименование работы': 'first',
        'Единица измерения': 'first',
        'Удельная стоимость, %': 'sum',
        'object_estimates_id': pd.Series.nunique
    }).reset_index().rename(columns={'object_estimates_id': 'Количество разных смет'})

    count_query = f"""
    SELECT 
        w.code AS "Код работы",
        COUNT(*) AS "Общее количество вхождения"
    FROM work w
    JOIN sections s ON w.local_section_id = s.id
    JOIN local_estimates le ON s.estimate_id = le.id
    JOIN object_estimates oe ON le.object_estimates_id = oe.id
    JOIN objects obj ON oe.object_id = obj.id
    {ids_filter}
    GROUP BY w.code
    """
    counts = pd.read_sql(count_query, engine)

    grouped = pd.merge(grouped, counts, on='Код работы', how='left')
    grouped = grouped.sort_values(['Количество разных смет', 'Общее количество вхождения'], ascending=[False, False])

    return grouped[[
        'Код работы', 'Наименование работы', 'Единица измерения',
        'Общее количество вхождения', 'Количество разных смет', 'Удельная стоимость, %'
    ]]


def process_materials_data(engine, object_ids=None):
    """
    Обрабатывает данные по материалам:
    - Группирует по полным кодам
    - Вычисляет удельную стоимость
    - Подсчитывает количество разных смет и общее количество вхождений
    """
    ids_filter = ''
    if object_ids:
        ids_filter = f"WHERE obj.id IN ({','.join(map(str, set(object_ids)))})"

    query = f"""
    SELECT 
        m.code AS "Код материала",
        m.name_material AS "Наименование материала",
        m.measurement_unit AS "Единица измерения",
        le.object_estimates_id,
        m.price AS "Стоимость материала",
        oe.object_estimates_price AS "Стоимость объектной сметы"
    FROM materials m
    JOIN work w ON m.work_id = w.id
    JOIN sections s ON w.local_section_id = s.id
    JOIN local_estimates le ON s.estimate_id = le.id
    JOIN object_estimates oe ON le.object_estimates_id = oe.id
    JOIN objects obj ON oe.object_id = obj.id
    {ids_filter}
    """
    df = pd.read_sql(query, engine)
    df['Удельная стоимость, %'] = df['Стоимость материала'] / df['Стоимость объектной сметы']

    grouped = df.groupby('Код материала').agg({
        'Наименование материала': 'first',
        'Единица измерения': 'first',
        'Удельная стоимость, %': 'sum',
        'object_estimates_id': pd.Series.nunique
    }).reset_index().rename(columns={'object_estimates_id': 'Количество разных смет'})

    count_query = f"""
    SELECT 
        m.code AS "Код материала",
        COUNT(*) AS "Общее количество вхождения"
    FROM materials m
    JOIN work w ON m.work_id = w.id
    JOIN sections s ON w.local_section_id = s.id
    JOIN local_estimates le ON s.estimate_id = le.id
    JOIN object_estimates oe ON le.object_estimates_id = oe.id
    JOIN objects obj ON oe.object_id = obj.id
    {ids_filter}
    GROUP BY m.code
    """
    counts = pd.read_sql(count_query, engine)

    grouped = pd.merge(grouped, counts, on='Код материала', how='left')
    grouped = grouped.sort_values(['Количество разных смет', 'Общее количество вхождения'], ascending=[False, False])

    return grouped[[
        'Код материала', 'Наименование материала', 'Единица измерения',
        'Общее количество вхождения', 'Количество разных смет', 'Удельная стоимость, %'
    ]]


def generate_report(db_params, filename=None, object_ids=None):
    """Генерирует отчет с сортировкой по количеству смет"""
    try:
        engine = create_engine(
            f"postgresql://{db_params['user']}:{quote_plus(db_params['password'])}@"
            f"{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        )

        if not object_ids:
            print("Не указаны ID объектов.")
            return False

        query = f"SELECT object_name FROM objects WHERE id IN ({','.join(map(str, set(object_ids)))})"
        df_names = pd.read_sql(query, engine)
        object_names = df_names['object_name'].tolist()

        works_df = process_work_data(engine, object_ids)
        if works_df.empty:
            print("Нет данных по работам для выбранных объектов.")
            return False

        materials_df = process_materials_data(engine, object_ids)
        if materials_df.empty:
            print("Нет данных по материалам для выбранных объектов.")
            return False

        if not filename:
            names_str = '_'.join([name.replace(' ', '_') for name in object_names[:3]])
            if len(object_names) > 3:
                names_str += f"_и_{len(object_names) - 3}_еще"
            filename = f"отчет_по_сметам_{names_str}.xlsx"

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            works_df.to_excel(writer, sheet_name='Работы', index=False)
            materials_df.to_excel(writer, sheet_name='Материалы', index=False)

            workbook = writer.book
            info_ws = workbook.create_sheet("Информация", 0)
            info_ws.append(["Отчет по объектам"])
            info_ws.append(["Количество объектов:", len(object_names)])
            info_ws.append(["Названия объектов:", ', '.join(object_names)])
            info_ws.append(["Дата создания:", pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')])

            for sheet_name in writer.sheets:
                ws = writer.sheets[sheet_name]

                if 'Удельная стоимость, %' in [cell.value for cell in ws[1]]:
                    col_idx = [cell.value for cell in ws[1]].index('Удельная стоимость, %') + 1
                    for row in ws.iter_rows(min_row=2, min_col=col_idx, max_col=col_idx):
                        for cell in row:
                            if isinstance(cell.value, (int, float)):
                                cell.number_format = '0.000000%'

                for col in ws.columns:
                    max_len = max(len(str(cell.value)) if cell.value else 0 for cell in col)
                    ws.column_dimensions[col[0].column_letter].width = max_len + 2

        print(f"\nОтчет по {len(object_names)} объектам сохранен в {filename}")
        return True

    except Exception as e:
        print(f"Ошибка при генерации отчета: {e}")
        return False

    finally:
        if 'engine' in locals():
            engine.dispose()
