import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus


def process_work_data(engine, object_ids=None):
    ids_filter = f"AND obj.id IN ({','.join(map(str, object_ids))})" if object_ids else ""

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

    if df.empty:
        return pd.DataFrame()

    df['Удельная стоимость, %'] = df['Стоимость работы'] / df['Стоимость объектной сметы']

    grouped = df.groupby('Код работы').agg({
        'Наименование работы': 'first',
        'Единица измерения': 'first',
        'Удельная стоимость, %': 'sum',
        'object_estimates_id': 'nunique'
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
    grouped = pd.merge(grouped, counts, on='Код работы')

    # Устанавливаем порядок столбцов: сначала "Общее количество вхождения", затем "Количество разных смет", потом "Удельная стоимость, %"
    columns = [
        'Код работы',
        'Наименование работы',
        'Единица измерения',
        'Общее количество вхождения',
        'Количество разных смет',
        'Удельная стоимость, %'
    ]
    grouped = grouped[columns]

    return grouped.sort_values(
        ['Удельная стоимость, %', 'Количество разных смет', 'Общее количество вхождения'],
        ascending=[False, False, False]
    )


def process_materials_data(engine, object_ids=None):
    ids_filter = f"AND obj.id IN ({','.join(map(str, object_ids))})" if object_ids else ""

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

    if df.empty:
        return pd.DataFrame()

    df['Удельная стоимость, %'] = df['Стоимость материала'] / df['Стоимость объектной сметы']

    grouped = df.groupby('Код материала').agg({
        'Наименование материала': 'first',
        'Единица измерения': 'first',
        'Удельная стоимость, %': 'sum',
        'object_estimates_id': 'nunique'
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
    grouped = pd.merge(grouped, counts, on='Код материала')

    # Устанавливаем порядок столбцов: сначала "Общее количество вхождения", затем "Количество разных смет", потом "Удельная стоимость, %"
    columns = [
        'Код материала',
        'Наименование материала',
        'Единица измерения',
        'Общее количество вхождения',
        'Количество разных смет',
        'Удельная стоимость, %'
    ]
    grouped = grouped[columns]

    return grouped.sort_values(
        ['Удельная стоимость, %', 'Количество разных смет', 'Общее количество вхождения'],
        ascending=[False, False, False]
    )



def generate_report(db_params, object_ids=None, filename=None):
    if not object_ids:
        print("Не указаны ID объектов.")
        return False

    try:
        engine = create_engine(
            f"postgresql://{db_params['user']}:{quote_plus(db_params['password'])}@"
            f"{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        )

        names_query = f"SELECT object_name FROM objects WHERE id IN ({','.join(map(str, object_ids))})"
        object_names = pd.read_sql(names_query, engine)['object_name'].tolist()

        works_df = process_work_data(engine, object_ids)
        materials_df = process_materials_data(engine, object_ids)

        if works_df.empty and materials_df.empty:
            print("Нет данных для отчета.")
            return False

        if not filename:
            base_name = '_'.join(name.replace(' ', '_') for name in object_names[:3])
            if len(object_names) > 3:
                base_name += f"_и_{len(object_names) - 3}_еще"
            filename = f"отчет_по_удельной_стоимости_{base_name}.xlsx"

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            if not works_df.empty:
                works_df.to_excel(writer, sheet_name='Работы', index=False)
            if not materials_df.empty:
                materials_df.to_excel(writer, sheet_name='Материалы', index=False)

            info_ws = writer.book.create_sheet("Информация", 0)
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
                            if isinstance(cell.value, (float, int)):
                                cell.number_format = '0.000000%'

                for col in ws.columns:
                    max_len = max(len(str(cell.value)) if cell.value else 0 for cell in col)
                    ws.column_dimensions[col[0].column_letter].width = max_len + 2

        print(f"\nОтчёт успешно сохранён: {filename}")
        return True

    except Exception as e:
        print(f"Ошибка при генерации отчета: {e}")
        return False

    finally:
        if 'engine' in locals():
            engine.dispose()
