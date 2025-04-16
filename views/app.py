import os
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from config import DB_CONFIG
from models.widgets import DragDropWidget
from models.processor import SmetaProcessor
from tkinterdnd2 import TkinterDnD
from reports.sorting_1 import generate_report as generate_all_entries_report
from reports.sorting_2 import generate_report as generate_estimates_report
from reports.sorting_3 import generate_report as generate_cost_report
from reports.ar_kr_procent import generate_report as generate_ar_kr_report

class SmetaApp(TkinterDnD.Tk):
    def __init__(self):
        super().__init__()
        self.title("Обработчик строительных смет")
        self.geometry("800x600")
        self.current_estimate_id = None
        self.current_object_id = None
        self.processor = SmetaProcessor()  # Инициализируем processor до создания UI

        # Инициализация интерфейса
        self.create_widgets()
        self.update_object_list()  # Обновляем список объектов при запуске

    def create_widgets(self):
        # Создаем вкладки
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # Вкладка для объектных смет
        self.object_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.object_tab, text="Объектные сметы")
        self.setup_object_tab()

        # Вкладка для локальных смет
        self.local_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.local_tab, text="Локальные сметы")
        self.setup_local_tab()

        # Вкладка анализа
        self.analysis_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.analysis_tab, text="Анализ")
        self.setup_analysis_tab()

        # Вкладка управления сметами
        self.management_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.management_tab, text="Управление сметами")
        self.setup_management_tab()

    def setup_object_tab(self):
        # Фрейм для ввода названия объекта
        self.object_name_frame = ttk.Frame(self.object_tab)
        self.object_name_frame.pack(fill=tk.X, padx=20, pady=10)

        ttk.Label(self.object_name_frame, text="Название объекта:").pack(side=tk.LEFT)
        self.object_name_entry = ttk.Entry(self.object_name_frame)
        self.object_name_entry.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=5)

        self.set_object_btn = ttk.Button(
            self.object_name_frame,
            text="Добавить объект",
            command=self.set_current_object
        )
        self.set_object_btn.pack(side=tk.LEFT, padx=5)

        # Виджет для перетаскивания файлов (важная часть!)
        self.object_drop_frame = ttk.Frame(self.object_tab)
        self.object_drop_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)

        self.object_drop = DragDropWidget(self.object_drop_frame)
        self.object_drop.pack(fill=tk.BOTH, expand=True)

        # Кнопка обработки
        self.process_object_btn = ttk.Button(
            self.object_tab,
            text="Добавить объектную смету",
            command=self.process_object_smeta
        )
        self.process_object_btn.pack(pady=5)

        # Кнопка следующего объекта
        self.next_object_btn = ttk.Button(
            self.object_tab,
            text="Следующий объект",
            command=self.next_object,
            state=tk.DISABLED
        )
        self.next_object_btn.pack(pady=5)

        # Лог операций
        self.object_log = tk.Text(self.object_tab, height=10, state="disabled")
        self.object_log.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)

    def setup_local_tab(self):
        # Заголовок списка
        ttk.Label(self.local_tab, text="Список необработанных локальных смет:",
                  font=('Arial', 10, 'bold')).pack(pady=(10, 5))

        # Виджет для перетаскивания файлов
        self.local_drop = DragDropWidget(self.local_tab)
        self.local_drop.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)

        # Фрейм для кнопок
        button_frame = ttk.Frame(self.local_tab)
        button_frame.pack(fill=tk.X, padx=20, pady=5)

        # Кнопка обработки
        self.process_local_btn = ttk.Button(
            button_frame,
            text="Обработать выбранную смету",
            command=self.process_local_smeta
        )
        self.process_local_btn.pack(side=tk.LEFT, padx=5)

        # Кнопка удаления
        self.delete_local_btn = ttk.Button(
            button_frame,
            text="Удалить выбранную смету",
            command=self.delete_selected_estimate
        )
        self.delete_local_btn.pack(side=tk.LEFT, padx=5)

        # Список необработанных смет (без отображения ID)
        self.local_listbox = tk.Listbox(
            self.local_tab,
            height=12,
            font=('Arial', 10),
            selectbackground="#4a6984",
            selectforeground="#ffffff"
        )
        self.local_listbox.pack(fill=tk.BOTH, expand=True, padx=20, pady=(0, 10))

        # Лог операций
        self.local_log = tk.Text(
            self.local_tab,
            height=8,
            state="disabled",
            wrap=tk.WORD,
            padx=5,
            pady=5
        )
        self.local_log.pack(fill=tk.BOTH, expand=True, padx=20, pady=(0, 10))

        # Обновляем список смет
        self.update_local_estimates_list()

    def setup_analysis_tab(self):
        main_frame = ttk.Frame(self.analysis_tab)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)

        # Заголовок
        ttk.Label(
            main_frame,
            text="Выберите объекты для анализа:",
            font=('Arial', 10, 'bold')
        ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 10))

        # Список объектов (без отображения ID)
        self.object_listbox = tk.Listbox(
            main_frame,
            height=12,
            selectmode=tk.MULTIPLE,
            font=('Arial', 10),
            selectbackground="#4a6984",
            selectforeground="#ffffff"
        )
        self.object_listbox.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(0, 10))

        # Управление выбором
        control_frame = ttk.Frame(main_frame)
        control_frame.grid(row=2, column=0, columnspan=2, sticky="ew", pady=5)

        ttk.Button(
            control_frame,
            text="Выбрать все",
            command=self.select_all_objects
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            control_frame,
            text="Сбросить выбор",
            command=self.clear_object_selection
        ).pack(side=tk.LEFT, padx=5)

        # Кнопки анализа
        analysis_buttons = [
            ("Анализ по вхождениям", self.run_all_entries_report),
            ("Анализ по количеству смет", self.run_estimates_report),
            ("Анализ по стоимости", self.run_cost_report),
            ("Анализ АР/КР", self.run_ar_kr_report)
        ]

        for i, (text, command) in enumerate(analysis_buttons):
            btn = ttk.Button(
                main_frame,
                text=text,
                command=command,
                width=25
            )
            btn.grid(row=3 + i // 2, column=i % 2, sticky="ew", padx=5, pady=5)

        # Настройка весов строк и столбцов
        main_frame.columnconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(1, weight=1)

        # Обновляем список объектов
        self.update_object_list()

    def setup_management_tab(self):
        """Настройка вкладки управления сметами"""
        main_frame = ttk.Frame(self.management_tab)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)

        # Дерево смет
        self.estimates_tree = ttk.Treeview(
            main_frame,
            columns=('type', 'price'),
            selectmode='browse',
            height=20
        )
        self.estimates_tree.heading('#0', text='Название')
        self.estimates_tree.heading('type', text='Тип')
        self.estimates_tree.heading('price', text='Стоимость')
        self.estimates_tree.column('type', width=100, anchor='center')
        self.estimates_tree.column('price', width=120, anchor='e')

        vsb = ttk.Scrollbar(main_frame, orient="vertical", command=self.estimates_tree.yview)
        hsb = ttk.Scrollbar(main_frame, orient="horizontal", command=self.estimates_tree.xview)
        self.estimates_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.estimates_tree.grid(row=0, column=0, sticky='nsew')
        vsb.grid(row=0, column=1, sticky='ns')
        hsb.grid(row=1, column=0, sticky='ew')

        # Кнопка удаления
        btn_frame = ttk.Frame(main_frame)
        btn_frame.grid(row=2, column=0, columnspan=2, pady=10)

        self.delete_btn = ttk.Button(
            btn_frame,
            text="Удалить выбранное",
            command=self.delete_selected_estimate_tree,
            state=tk.DISABLED
        )
        self.delete_btn.pack(side=tk.LEFT, padx=5)

        # Настройка весов
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(0, weight=1)

        # Привязка событий
        self.estimates_tree.bind('<<TreeviewSelect>>', self.on_tree_select)

        # Заполнение дерева
        self.update_estimates_tree()

    def log_message(self, widget, message):
        widget.config(state="normal")
        widget.insert("end", message + "\n")
        widget.see("end")
        widget.config(state="disabled")

    def set_current_object(self):
        object_name = self.object_name_entry.get().strip()
        if not object_name:
            messagebox.showerror("Ошибка", "Введите название объекта")
            return

        try:
            with self.processor as p:
                cursor = p.conn.cursor()
                cursor.execute("SELECT id FROM objects WHERE object_name = %s", (object_name,))
                existing_object = cursor.fetchone()

                if existing_object:
                    self.current_object_id = existing_object[0]
                    self.log_message(self.object_log,
                                     f"Выбран существующий объект: {object_name} (ID: {self.current_object_id})")
                else:
                    cursor.execute(
                        "INSERT INTO objects (object_name) VALUES (%s) RETURNING id",
                        (object_name,)
                    )
                    self.current_object_id = cursor.fetchone()[0]
                    p.conn.commit()
                    self.log_message(self.object_log,
                                     f"Создан новый объект: {object_name} (ID: {self.current_object_id})")

                self.next_object_btn.config(state=tk.NORMAL)
                self.object_name_entry.config(state=tk.DISABLED)
                self.set_object_btn.config(state=tk.DISABLED)
                self.update_object_list()  # Обновляем список объектов после изменения

        except Exception as e:
            self.log_message(self.object_log, f"Ошибка: {str(e)}")
            messagebox.showerror("Ошибка", str(e))

    def next_object(self):
        self.current_object_id = None
        self.object_name_entry.config(state=tk.NORMAL)
        self.set_object_btn.config(state=tk.NORMAL)
        self.next_object_btn.config(state=tk.DISABLED)
        self.object_name_entry.delete(0, tk.END)
        self.object_drop.reset_widget()
        self.object_drop.config(
            text="Перетащите файл сюда или нажмите для выбора",
            background="#f0f0f0"
        )
        self.log_message(self.object_log, "Готово к вводу нового объекта")
        self.refresh_all_lists()
        self.update_object_list()

    def process_object_smeta(self):
        if not self.current_object_id:
            messagebox.showerror("Ошибка", "Сначала установите объект")
            return

        file_path = self.object_drop.get_file()
        if not file_path:
            messagebox.showerror("Ошибка", "Файл не выбран или не существует")
            self.object_drop.reset_widget()
            return

        try:
            self.log_message(self.object_log, f"Обработка файла: {os.path.basename(file_path)}")

            with self.processor as p:
                success = p.process_object_smeta(file_path, self.current_object_id)

            if success:
                self.log_message(self.object_log, "✅ Объектная смета успешно добавлена!")
                self.refresh_all_lists()
                self.notebook.select(self.local_tab)
            else:
                self.log_message(self.object_log, "❌ Ошибка обработки объектной сметы")

            self.object_drop.reset_widget()

        except Exception as e:
            error_msg = f"Ошибка: {str(e)}"
            self.log_message(self.object_log, f"❌ {error_msg}")
            messagebox.showerror("Ошибка", error_msg)
            self.object_drop.reset_widget()

    def process_local_smeta(self):
        if not self.current_estimate_id:
            messagebox.showerror("Ошибка", "Сначала выберите смету из списка")
            return

        file_path = self.local_drop.get_file()
        if not file_path:
            messagebox.showerror("Ошибка", "Файл не выбран")
            return

        try:
            self.log_message(self.local_log, f"Обработка файла: {file_path}")
            with self.processor as p:
                success, total_cost = p.process_xml_estimate(file_path, self.current_estimate_id)

            if success:
                self.log_message(self.local_log,
                                 f"Локальная смета успешно обработана! Стоимость: {total_cost:.2f} руб.")
                self.refresh_all_lists()
                with self.processor as p:
                    p.update_estimate_price(self.current_estimate_id, total_cost)
                self.update_local_estimates_list()
            else:
                self.log_message(self.local_log, "Ошибка обработки локальной сметы")

        except Exception as e:
            self.log_message(self.local_log, f"Ошибка: {str(e)}")
            messagebox.showerror("Ошибка", str(e))

    def update_local_estimates_list(self):
        self.local_listbox.delete(0, tk.END)
        try:
            with self.processor as p:
                estimates = p.get_unprocessed_local_estimates()

            if not estimates:
                self.local_listbox.insert(tk.END, "Нет необработанных локальных смет")
                return

            for idx, (id, name, obj_estimate_name, obj_name, _) in enumerate(estimates, 1):
                self.local_listbox.insert(tk.END, f"{idx}. {name} (Объект: {obj_name}, Смета: {obj_estimate_name})")

            self.local_listbox.bind("<<ListboxSelect>>", self.on_estimate_select)
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось загрузить список смет: {str(e)}")

    def on_estimate_select(self, event):
        selection = event.widget.curselection()
        if selection:
            index = selection[0]
            try:
                with self.processor as p:
                    estimates = p.get_unprocessed_local_estimates()
                if estimates and index < len(estimates):
                    self.current_estimate_id = estimates[index][0]
                    self.log_message(self.local_log, f"Выбрана смета ID: {self.current_estimate_id}")
            except Exception as e:
                messagebox.showerror("Ошибка", f"Не удалось выбрать смету: {str(e)}")

    def update_object_list(self):
        """Обновление списка объектов с нумерацией"""
        try:
            with self.processor as p:
                conn = p.conn
                with conn.cursor() as cursor:
                    cursor.execute("SELECT id, object_name FROM objects ORDER BY id ASC")
                    self.objects = cursor.fetchall()

            self.object_listbox.delete(0, tk.END)
            for idx, (obj_id, name) in enumerate(self.objects, 1):
                # Добавляем порядковый номер перед названием объекта
                self.object_listbox.insert(tk.END, f"{idx}. {name}")

        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось загрузить объекты: {str(e)}")

    def get_selected_object_ids(self):
        selected_indices = self.object_listbox.curselection()
        if not selected_indices:
            messagebox.showwarning("Внимание", "Выберите хотя бы один объект")
            return []
        # Получаем реальные ID из сохраненного списка объектов
        return [self.objects[i][0] for i in selected_indices]

    def select_all_objects(self):
        self.object_listbox.select_set(0, tk.END)

    def clear_object_selection(self):
        self.object_listbox.selection_clear(0, tk.END)

    def run_all_entries_report(self):
        ids = self.get_selected_object_ids()
        if ids:
            filename = filedialog.asksaveasfilename(defaultextension=".xlsx",
                                                    initialfile="анализ_вхождения_по_вхождениям.xlsx")
            if filename:
                generate_all_entries_report(DB_CONFIG, filename, object_ids=ids)
                messagebox.showinfo("Готово", "Отчет по вхождениям сформирован!")

    def run_estimates_report(self):
        ids = self.get_selected_object_ids()
        if ids:
            filename = filedialog.asksaveasfilename(defaultextension=".xlsx",
                                                    initialfile="анализ_смет_по_количеству_смет.xlsx")
            if filename:
                generate_estimates_report(DB_CONFIG, filename, object_ids=ids)
                messagebox.showinfo("Готово", "Отчет по количеству смет сформирован!")

    def run_cost_report(self):
        ids = self.get_selected_object_ids()
        if ids:
            filename = filedialog.asksaveasfilename(defaultextension=".xlsx",
                                                    initialfile="анализ_смет_по_удельной_стоимости.xlsx")
            if filename:
                generate_cost_report(DB_CONFIG, object_ids=ids, filename=filename)
                messagebox.showinfo("Готово", "Отчет по удельной стоимости сформирован!")

    def run_ar_kr_report(self):
        filename = filedialog.asksaveasfilename(defaultextension=".xlsx", initialfile="анализ_ар_кр.xlsx")
        if filename:
            generate_ar_kr_report(DB_CONFIG, filename)
            messagebox.showinfo("Готово", "Отчет по АР/КР сформирован!")

    def delete_selected_estimate(self):
        selection = self.local_listbox.curselection()
        if not selection:
            messagebox.showwarning("Внимание", "Выберите смету для удаления")
            return

        index = selection[0]

        try:
            processor = SmetaProcessor()
            processor.conn = processor.get_db_connection()

            estimates = processor.get_unprocessed_local_estimates()
            if not estimates or index >= len(estimates):
                messagebox.showerror("Ошибка", "Смета не найдена в базе")
                processor.conn.close()
                return

            estimate_id, estimate_name, oe_name, obj_name, oe_id = estimates[index]

            confirm = messagebox.askyesno(
                "Подтверждение",
                f"Удалить смету '{estimate_name}' (ID: {estimate_id})?"
            )
            if not confirm:
                processor.conn.close()
                return

            try:
                with processor.conn.cursor() as cursor:
                    # Получаем ID объектной сметы
                    cursor.execute(
                        "SELECT object_estimates_id FROM local_estimates WHERE id = %s",
                        (estimate_id,)
                    )
                    oe_id = cursor.fetchone()[0]

                    # 1. Удаляем связанные материалы, работы и разделы
                    # (существующий код удаления)

                    # 2. Удаляем саму локальную смету
                    cursor.execute(
                        "DELETE FROM local_estimates WHERE id = %s",
                        (estimate_id,)
                    )

                    # 3. Проверяем, остались ли другие локальные сметы у объектной сметы
                    cursor.execute(
                        "SELECT COUNT(*) FROM local_estimates WHERE object_estimates_id = %s",
                        (oe_id,)
                    )
                    remaining_local = cursor.fetchone()[0]

                    if remaining_local == 0:
                        # Если нет других локальных смет - удаляем объектную смету
                        cursor.execute(
                            "DELETE FROM object_estimates WHERE id = %s RETURNING object_id",
                            (oe_id,)
                        )
                        deleted_oe = cursor.fetchone()

                        if deleted_oe:
                            object_id = deleted_oe[0]

                            # Проверяем, остались ли другие объектные сметы у объекта
                            cursor.execute(
                                "SELECT COUNT(*) FROM object_estimates WHERE object_id = %s",
                                (object_id,)
                            )
                            remaining_object = cursor.fetchone()[0]

                            if remaining_object == 0:
                                # Если нет других объектных смет - удаляем объект
                                cursor.execute(
                                    "DELETE FROM objects WHERE id = %s",
                                    (object_id,)
                                )
                                self.log_message(self.local_log,
                                                 f"Удален объект ID: {object_id}, так как не осталось смет")

                    processor.conn.commit()
                    self.log_message(self.local_log, f"Удалена локальная смета: {estimate_name}")
                    self.refresh_all_lists()

            except Exception as e:
                processor.conn.rollback()
                messagebox.showerror("Ошибка", f"Не удалось удалить смету: {str(e)}")
            finally:
                processor.conn.close()

        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось подключиться к базе данных: {str(e)}")

    def update_estimates_tree(self):
        """Обновление дерева смет с нумерацией объектов"""
        self.estimates_tree.delete(*self.estimates_tree.get_children())

        try:
            with self.processor as p:
                # Получаем все объекты, сортированные по ID
                cursor = p.conn.cursor()
                cursor.execute("SELECT id, object_name FROM objects ORDER BY id")
                objects = cursor.fetchall()

                for idx, (obj_id, obj_name) in enumerate(objects, 1):
                    # Добавляем порядковый номер к названию объекта
                    obj_node = self.estimates_tree.insert(
                        '', 'end',
                        text=f"{idx}. {obj_name}",  # Вот здесь добавляем номер
                        values=('Объект', ''),
                        iid=f'obj_{obj_id}',
                        open=True
                    )

                    # Остальной код остаётся без изменений...
                    cursor.execute(
                        "SELECT id, name_object_estimate, object_estimates_price "
                        "FROM object_estimates WHERE object_id = %s ORDER BY name_object_estimate",
                        (obj_id,)
                    )
                    obj_estimates = cursor.fetchall()

                    for oe_id, oe_name, oe_price in obj_estimates:
                        oe_node = self.estimates_tree.insert(
                            obj_node, 'end',
                            text=oe_name,
                            values=('Объектная смета', f"{oe_price:,.2f} руб."),
                            iid=f'oe_{oe_id}',
                            open=True
                        )

                        cursor.execute(
                            "SELECT id, name_local_estimate, local_estimates_price "
                            "FROM local_estimates WHERE object_estimates_id = %s "
                            "ORDER BY name_local_estimate",
                            (oe_id,)
                        )
                        local_estimates = cursor.fetchall()

                        for le_id, le_name, le_price in local_estimates:
                            price_str = f"{le_price:,.2f} руб." if le_price else "Не обработана"
                            self.estimates_tree.insert(
                                oe_node, 'end',
                                text=le_name,
                                values=('Локальная смета', price_str),
                                iid=f'le_{le_id}'
                            )

        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось загрузить данные: {str(e)}")

    def on_tree_select(self, event):
        """Обработка выбора элемента в дереве"""
        selected = self.estimates_tree.selection()
        if selected:
            self.delete_btn.config(state=tk.NORMAL)
        else:
            self.delete_btn.config(state=tk.DISABLED)

    def delete_selected_estimate_tree(self):
        """Удаление выбранной сметы с каскадным удалением"""
        selected = self.estimates_tree.selection()
        if not selected:
            return

        item_id = selected[0]

        if item_id.startswith('le_'):
            # Удаление локальной сметы
            le_id = int(item_id[3:])
            confirm = messagebox.askyesno(
                "Подтверждение",
                "Удалить локальную смету и все связанные разделы, работы и материалы?"
            )
            if confirm:
                try:
                    with self.processor as p:
                        # Удаляем материалы, работы и разделы для этой сметы
                        cursor = p.conn.cursor()

                        # 1. Находим все разделы для этой сметы
                        cursor.execute("SELECT id FROM sections WHERE estimate_id = %s", (le_id,))
                        section_ids = [row[0] for row in cursor.fetchall()]

                        if section_ids:
                            # 2. Находим все работы для этих разделов
                            cursor.execute(
                                "SELECT id FROM work WHERE local_section_id IN %s",
                                (tuple(section_ids),)
                            )
                            work_ids = [row[0] for row in cursor.fetchall()]

                            if work_ids:
                                # 3. Удаляем все материалы для этих работ
                                cursor.execute(
                                    "DELETE FROM materials WHERE work_id IN %s",
                                    (tuple(work_ids),)
                                )

                            # 4. Удаляем все работы
                            cursor.execute(
                                "DELETE FROM work WHERE local_section_id IN %s",
                                (tuple(section_ids),)
                            )

                        # 5. Удаляем все разделы
                        cursor.execute("DELETE FROM sections WHERE estimate_id = %s", (le_id,))

                        # 6. Получаем ID объектной сметы для проверки после удаления
                        cursor.execute(
                            "SELECT object_estimates_id FROM local_estimates WHERE id = %s",
                            (le_id,)
                        )
                        oe_id = cursor.fetchone()[0]

                        # 7. Удаляем локальную смету
                        cursor.execute("DELETE FROM local_estimates WHERE id = %s", (le_id,))

                        # 8. Проверяем, остались ли другие локальные сметы у объектной сметы
                        cursor.execute(
                            "SELECT COUNT(*) FROM local_estimates WHERE object_estimates_id = %s",
                            (oe_id,)
                        )
                        remaining_le = cursor.fetchone()[0]

                        if remaining_le == 0:
                            # Если нет других локальных смет - удаляем объектную смету
                            cursor.execute(
                                "SELECT object_id FROM object_estimates WHERE id = %s",
                                (oe_id,)
                            )
                            obj_id = cursor.fetchone()[0]

                            cursor.execute(
                                "DELETE FROM object_estimates WHERE id = %s",
                                (oe_id,)
                            )

                            # Проверяем, остались ли другие объектные сметы у объекта
                            cursor.execute(
                                "SELECT COUNT(*) FROM object_estimates WHERE object_id = %s",
                                (obj_id,)
                            )
                            remaining_oe = cursor.fetchone()[0]

                            if remaining_oe == 0:
                                # Если нет других объектных смет - удаляем объект
                                cursor.execute(
                                    "DELETE FROM objects WHERE id = %s",
                                    (obj_id,)
                                )

                        p.conn.commit()
                        self.refresh_all_lists()
                        messagebox.showinfo("Успех", "Локальная смета и все связанные данные удалены")

                except Exception as e:
                    messagebox.showerror("Ошибка", f"Не удалось удалить смету: {str(e)}")

        elif item_id.startswith('oe_'):
            # Удаление объектной сметы (со всеми локальными)
            oe_id = int(item_id[3:])
            confirm = messagebox.askyesno(
                "Подтверждение",
                "Удалить объектную смету и все связанные локальные сметы, разделы, работы и материалы?"
            )
            if confirm:
                try:
                    with self.processor as p:
                        cursor = p.conn.cursor()

                        # 1. Находим все локальные сметы для этой объектной сметы
                        cursor.execute(
                            "SELECT id FROM local_estimates WHERE object_estimates_id = %s",
                            (oe_id,)
                        )
                        le_ids = [row[0] for row in cursor.fetchall()]

                        if le_ids:
                            # 2. Находим все разделы для этих смет
                            cursor.execute(
                                "SELECT id FROM sections WHERE estimate_id IN %s",
                                (tuple(le_ids),)
                            )
                            section_ids = [row[0] for row in cursor.fetchall()]

                            if section_ids:
                                # 3. Находим все работы для этих разделов
                                cursor.execute(
                                    "SELECT id FROM work WHERE local_section_id IN %s",
                                    (tuple(section_ids),)
                                )
                                work_ids = [row[0] for row in cursor.fetchall()]

                                if work_ids:
                                    # 4. Удаляем все материалы для этих работ
                                    cursor.execute(
                                        "DELETE FROM materials WHERE work_id IN %s",
                                        (tuple(work_ids),)
                                    )

                                # 5. Удаляем все работы
                                cursor.execute(
                                    "DELETE FROM work WHERE local_section_id IN %s",
                                    (tuple(section_ids),)
                                )

                            # 6. Удаляем все разделы
                            cursor.execute(
                                "DELETE FROM sections WHERE estimate_id IN %s",
                                (tuple(le_ids),)
                            )

                        # 7. Удаляем все локальные сметы
                        cursor.execute(
                            "DELETE FROM local_estimates WHERE object_estimates_id = %s",
                            (oe_id,)
                        )

                        # 8. Получаем ID объекта для проверки после удаления
                        cursor.execute(
                            "SELECT object_id FROM object_estimates WHERE id = %s",
                            (oe_id,)
                        )
                        obj_id = cursor.fetchone()[0]

                        # 9. Удаляем объектную смету
                        cursor.execute(
                            "DELETE FROM object_estimates WHERE id = %s",
                            (oe_id,)
                        )

                        # 10. Проверяем, остались ли другие объектные сметы у объекта
                        cursor.execute(
                            "SELECT COUNT(*) FROM object_estimates WHERE object_id = %s",
                            (obj_id,)
                        )
                        remaining_oe = cursor.fetchone()[0]

                        if remaining_oe == 0:
                            # Если нет других объектных смет - удаляем объект
                            cursor.execute(
                                "DELETE FROM objects WHERE id = %s",
                                (obj_id,)
                            )

                        p.conn.commit()
                        self.refresh_all_lists()
                        messagebox.showinfo("Успех", "Объектная смета и все связанные данные удалены")

                except Exception as e:
                    messagebox.showerror("Ошибка", f"Не удалось удалить смету: {str(e)}")

        elif item_id.startswith('obj_'):
            # Удаление объекта (со всеми объектными и локальными сметами)
            obj_id = int(item_id[4:])
            confirm = messagebox.askyesno(
                "Подтверждение",
                "Удалить объект и все связанные объектные и локальные сметы, разделы, работы и материалы?"
            )
            if confirm:
                try:
                    with self.processor as p:
                        cursor = p.conn.cursor()

                        # 1. Находим все объектные сметы для этого объекта
                        cursor.execute(
                            "SELECT id FROM object_estimates WHERE object_id = %s",
                            (obj_id,)
                        )
                        oe_ids = [row[0] for row in cursor.fetchall()]

                        if oe_ids:
                            # 2. Находим все локальные сметы для этих объектных смет
                            cursor.execute(
                                "SELECT id FROM local_estimates WHERE object_estimates_id IN %s",
                                (tuple(oe_ids),)
                            )
                            le_ids = [row[0] for row in cursor.fetchall()]

                            if le_ids:
                                # 3. Находим все разделы для этих смет
                                cursor.execute(
                                    "SELECT id FROM sections WHERE estimate_id IN %s",
                                    (tuple(le_ids),)
                                )
                                section_ids = [row[0] for row in cursor.fetchall()]

                                if section_ids:
                                    # 4. Находим все работы для этих разделов
                                    cursor.execute(
                                        "SELECT id FROM work WHERE local_section_id IN %s",
                                        (tuple(section_ids),)
                                    )
                                    work_ids = [row[0] for row in cursor.fetchall()]

                                    if work_ids:
                                        # 5. Удаляем все материалы для этих работ
                                        cursor.execute(
                                            "DELETE FROM materials WHERE work_id IN %s",
                                            (tuple(work_ids),)
                                        )

                                    # 6. Удаляем все работы
                                    cursor.execute(
                                        "DELETE FROM work WHERE local_section_id IN %s",
                                        (tuple(section_ids),)
                                    )

                                # 7. Удаляем все разделы
                                cursor.execute(
                                    "DELETE FROM sections WHERE estimate_id IN %s",
                                    (tuple(le_ids),)
                                )

                            # 8. Удаляем все локальные сметы
                            cursor.execute(
                                "DELETE FROM local_estimates WHERE object_estimates_id IN %s",
                                (tuple(oe_ids),)
                            )

                        # 9. Удаляем все объектные сметы
                        cursor.execute(
                            "DELETE FROM object_estimates WHERE object_id = %s",
                            (obj_id,)
                        )

                        # 10. Удаляем объект
                        cursor.execute(
                            "DELETE FROM objects WHERE id = %s",
                            (obj_id,)
                        )

                        p.conn.commit()
                        self.refresh_all_lists()
                        messagebox.showinfo("Успех", "Объект и все связанные данные удалены")

                except Exception as e:
                    messagebox.showerror("Ошибка", f"Не удалось удалить объект: {str(e)}")

    def refresh_all_lists(self):
        """Обновляет все списки во всех разделах"""
        self.update_estimates_tree()  # Обновляем дерево в разделе "Управление сметами"
        self.update_object_list()  # Обновляем список объектов в разделе "Анализ"
        self.update_local_estimates_list()  # Обновляем список локальных смет