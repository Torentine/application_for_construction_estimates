import os
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinterdnd2 import TkinterDnD, DND_FILES
import psycopg2
from psycopg2 import sql
from parsing_object_smeta import parse_and_save_smeta
from type_opr import identify_file_type
from parsing_xml_db import parse_xml_estimate

# Конфигурация подключения к БД
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "qwerty123!",
    "host": "localhost",
    "port": "5432"
}


class DragDropWidget(tk.Label):
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        self.file_path = None
        self.configure(
            text="Перетащите файл сюда или нажмите для выбора",
            relief="solid",
            padx=20,
            pady=20,
            background="#f0f0f0"
        )
        self.setup_dnd()

    def setup_dnd(self):
        # Настройка обработчиков событий
        self.bind("<Button-1>", self.select_file)
        self.drop_target_register(DND_FILES)
        self.dnd_bind('<<Drop>>', self.on_drop)

    def select_file(self, event=None):
        file_path = filedialog.askopenfilename()
        if file_path:
            self.set_file(file_path)

    def on_drop(self, event):
        # Обработка перетаскивания файла
        file_path = event.data.strip()
        if file_path.startswith('{') and file_path.endswith('}'):
            file_path = file_path[1:-1]  # Удаляем фигурные скобки для Windows
        if os.path.exists(file_path):
            self.set_file(file_path)
        else:
            messagebox.showerror("Ошибка", "Файл не найден")

    def set_file(self, file_path):
        self.file_path = file_path
        self.config(
            text=f"Выбран файл:\n{os.path.basename(file_path)}",
            background="#e0f0e0"
        )

    def get_file(self):
        return self.file_path


class SmetaApp(TkinterDnD.Tk):
    def __init__(self):
        super().__init__()
        self.title("Обработчик строительных смет")
        self.geometry("800x600")
        self.current_estimate_id = None

        # Инициализируем процессор
        self.processor = SmetaProcessor()
        self.setup_ui()

    def setup_ui(self):
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

    def setup_object_tab(self):
        # Виджет для перетаскивания файлов
        self.object_drop = DragDropWidget(self.object_tab)
        self.object_drop.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)

        # Кнопка обработки
        self.process_object_btn = ttk.Button(
            self.object_tab,
            text="Обработать объектную смету",
            command=self.process_object_smeta
        )
        self.process_object_btn.pack(pady=10)

        # Лог операций
        self.object_log = tk.Text(self.object_tab, height=10, state="disabled")
        self.object_log.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)

    def setup_local_tab(self):
        # Виджет для перетаскивания файлов
        self.local_drop = DragDropWidget(self.local_tab)
        self.local_drop.pack(fill=tk.BOTH, expand=True, padx=20, pady=20)

        # Кнопка обработки
        self.process_local_btn = ttk.Button(
            self.local_tab,
            text="Обработать локальную смету",
            command=self.process_local_smeta
        )
        self.process_local_btn.pack(pady=10)

        # Список необработанных смет
        self.local_listbox = tk.Listbox(self.local_tab, height=10)
        self.local_listbox.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)

        # Лог операций
        self.local_log = tk.Text(self.local_tab, height=10, state="disabled")
        self.local_log.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)

        # Обновляем список смет
        self.update_local_estimates_list()

    def log_message(self, widget, message):
        widget.config(state="normal")
        widget.insert("end", message + "\n")
        widget.see("end")
        widget.config(state="disabled")

    def process_object_smeta(self):
        file_path = self.object_drop.get_file()
        if not file_path:
            messagebox.showerror("Ошибка", "Файл не выбран")
            return

        try:
            self.log_message(self.object_log, f"Обработка файла: {file_path}")
            with self.processor as p:
                success = p.process_object_smeta(file_path)

            if success:
                self.log_message(self.object_log, "Объектная смета успешно обработана!")
                self.update_local_estimates_list()
                self.notebook.select(self.local_tab)
            else:
                self.log_message(self.object_log, "Ошибка обработки объектной сметы")

        except Exception as e:
            self.log_message(self.object_log, f"Ошибка: {str(e)}")
            messagebox.showerror("Ошибка", str(e))

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

            for idx, (id, name, _, _, _) in enumerate(estimates, 1):
                self.local_listbox.insert(tk.END, f"{idx}. {name} (ID: {id})")

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
        """Устанавливает соединение с PostgreSQL"""
        try:
            return psycopg2.connect(**DB_CONFIG)
        except psycopg2.Error as e:
            raise Exception(f"Ошибка подключения к базе данных: {e}")

    def get_unprocessed_local_estimates(self):
        """Получаем список необработанных локальных смет (где price IS NULL)"""
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
        """Обновляем цену локальной сметы"""
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

    def process_object_smeta(self, file_path):
        """Обрабатываем объектную смету"""
        if not file_path:
            raise Exception("Объектная смета не указана")

        file_type = identify_file_type(file_path)
        print(f"Тип файла объектной сметы: {file_type}")

        if file_type == "XLSX":
            return parse_and_save_smeta(file_path)
        elif file_type in ["XLS", "XML", "GGE"]:
            print(f"Обработка формата {file_type} для объектной сметы пока в разработке")
            return False
        else:
            raise Exception(f"Формат {file_type} не поддерживается для объектной сметы")

    def process_xml_estimate(self, xml_path, estimate_id):
        """Обработка XML сметы"""
        try:
            estimate_data = parse_xml_estimate(
                xml_file_path=xml_path,
                db_params=DB_CONFIG,
                estimate_id=estimate_id
            )
            return True, estimate_data['total_cost']
        except Exception as e:
            raise Exception(f"Ошибка обработки XML: {str(e)}")


if __name__ == "__main__":
    app = SmetaApp()
    app.mainloop()