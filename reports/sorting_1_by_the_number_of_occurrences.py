import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

def get_objects_list(engine):
    """Получаем список объектов из базы данных"""
    query = "SELECT id, object_name FROM objects ORDER BY object_name"
    return pd.read_sql(query, engine)

def select_objects(objects_df):
    """Выбор объектов пользователем (можно выбрать несколько)"""
    print("\nСписок доступных объектов:")
    for _, row in objects_df.iterrows():
        print(f"{row['id']}. {row['object_name']}")

    print("\nВведите номера объектов через запятую (например: 1,3,5) или 0 для отмены")

    while True:
        try:
            choice = input("Ваш выбор: ").strip()
            if choice == '0':
                return [], []

            selected_ids = [int(id_str.strip()) for id_str in choice.split(',')]

            # Проверяем, что все введенные ID существуют
            invalid_ids = [id_ for id_ in selected_ids if id_ not in objects_df['id'].values]
            if invalid_ids:
                print(f"Ошибка: следующие ID не найдены: {invalid_ids}")
                continue

            selected_objects = objects_df[objects_df['id'].isin(selected_ids)]
            return selected_ids, selected_objects['object_name'].tolist()

        except ValueError:
            print("Ошибка: введите номера через запятую (например: 1,3,5)")

def process_work_data(engine, object_ids):
    """Обрабатываем данные по работам для выбранных объектов"""
    ids_str = ','.join(map(str, object_ids))
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
    WHERE obj.id IN ({ids_str})
    """
    df = pd.read_sql(query, engine)

    df['Удельная стоимость, %'] = (df['Стоимость работы'] / df['Стоимость объектной сметы'])

    grouped = df.groupby(['Код работы']).agg({
        'Наименование работы': 'first',
        'Единица измерения': 'first',
        'Удельная стоимость, %': 'sum',
        'object_estimates_id': lambda x: x.nunique()
    }).reset_index()

    grouped = grouped.rename(columns={'object_estimates_id': 'Количество разных смет'})

    count_query = f"""
    SELECT 
        w.code AS "Код работы",
        COUNT(*) AS "Общее количество вхождения"
    FROM work w
    JOIN sections s ON w.local_section_id = s.id
    JOIN local_estimates le ON s.estimate_id = le.id
    JOIN object_estimates oe ON le.object_estimates_id = oe.id
    JOIN objects obj ON oe.object_id = obj.id
    WHERE obj.id IN ({ids_str})
    GROUP BY w.code
    """
    counts = pd.read_sql(count_query, engine)
    grouped = pd.merge(grouped, counts, on=['Код работы'])

    grouped = grouped.sort_values(['Общее количество вхождения'], ascending=[False])

    return grouped[['Код работы', 'Наименование работы', 'Единица измерения',
                    'Общее количество вхождения', 'Количество разных смет',
                    'Удельная стоимость, %']]

def process_materials_data(engine, object_ids):
    """Обрабатываем данные по материалам для выбранных объектов"""
    ids_str = ','.join(map(str, object_ids))
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
    WHERE obj.id IN ({ids_str})
    """
    df = pd.read_sql(query, engine)

    df['Удельная стоимость, %'] = (df['Стоимость материала'] / df['Стоимость объектной сметы'])

    grouped = df.groupby(['Код материала']).agg({
        'Наименование материала': 'first',
        'Единица измерения': 'first',
        'Удельная стоимость, %': 'sum',
        'object_estimates_id': lambda x: x.nunique()
    }).reset_index()

    grouped = grouped.rename(columns={'object_estimates_id': 'Количество разных смет'})

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
    WHERE obj.id IN ({ids_str})
    GROUP BY m.code
    """
    counts = pd.read_sql(count_query, engine)
    grouped = pd.merge(grouped, counts, on=['Код материала'])

    grouped = grouped.sort_values(['Общее количество вхождения'], ascending=[False])

    return grouped[['Код материала', 'Наименование материала', 'Единица измерения',
                    'Общее количество вхождения', 'Количество разных смет',
                    'Удельная стоимость, %']]

def generate_report(db_params, filename=None, object_ids=None):
    """Генерирует отчет для выбранных объектов"""
    try:
        engine = create_engine(
            f"postgresql://{db_params['user']}:{quote_plus(db_params['password'])}@"
            f"{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
        )

        if object_ids is None:
            objects_df = get_objects_list(engine)
            if objects_df.empty:
                print("В базе данных нет объектов для отчета.")
                return False

            object_ids, object_names = select_objects(objects_df)
            if not object_ids:
                return False
        else:
            # Получаем названия объектов по их ID
            query = f"SELECT object_name FROM objects WHERE id IN ({','.join(map(str, object_ids))})"
            df_names = pd.read_sql(query, engine)
            object_names = df_names['object_name'].tolist()

        # Обрабатываем работы
        works_df = process_work_data(engine, object_ids)
        if works_df.empty:
            print("Нет данных по работам для выбранных объектов.")
            return False

        # Обрабатываем материалы
        materials_df = process_materials_data(engine, object_ids)
        if materials_df.empty:
            print("Нет данных по материалам для выбранных объектов.")
            return False

        if not filename:
            names_str = '_'.join([name.replace(' ', '_') for name in object_names[:3]])
            if len(object_names) > 3:
                names_str += f"_и_{len(object_names) - 3}_еще"
            filename = f"отчет_по_объектам_{names_str}.xlsx"

        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Пишем данные по работам
            works_df.to_excel(writer, sheet_name='Работы', index=False)

            # Пишем данные по материалам
            materials_df.to_excel(writer, sheet_name='Материалы', index=False)

            # Добавляем информацию об объектах
            workbook = writer.book
            worksheet = workbook.create_sheet("Информация", 0)
            worksheet.append(["Отчет по объектам"])
            worksheet.append(["Количество объектов:", len(object_names)])
            worksheet.append(["Названия объектов:", ', '.join(object_names)])
            worksheet.append(["Дата создания:", pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')])

            # Форматируем столбцы
            for sheet_name in writer.sheets:
                ws = writer.sheets[sheet_name]

                if 'Удельная стоимость, %' in [cell.value for cell in ws[1]]:  # проверка на столбец с форматом
                    col_idx = [cell.value for cell in ws[1]].index('Удельная стоимость, %') + 1
                    for row in ws.iter_rows(min_row=2, min_col=col_idx, max_col=col_idx):
                        for cell in row:
                            if isinstance(cell.value, (int, float)):
                                cell.number_format = '0.000000%'

                for col in ws.columns:
                    max_len = max(len(str(cell.value)) for cell in col)
                    ws.column_dimensions[col[0].column_letter].width = max_len + 2

        print(f"\nОтчет по {len(object_names)} объектам сохранен в {filename}")
        return True

    except Exception as e:
        print(f"Ошибка при генерации отчета: {e}")
        return False
    finally:
        if 'engine' in locals():
            engine.dispose()

