import os
import tkinter as tk
from tkinter import filedialog, messagebox
from tkinterdnd2 import DND_FILES

class DragDropWidget(tk.Label):
    def __init__(self, master, **kwargs):
        super().__init__(master, **kwargs)
        self.file_path = None
        self.configure(
            text="Перетащите файл сюда или нажмите для выбора",
            relief="solid",
            borderwidth=2,
            padx=40,
            pady=30,
            background="#f0f0f0",
            foreground="#333333",
            cursor="hand2"  # Изменяем курсор при наведении
        )
        self.setup_dnd()
        self.bind_events()

    def setup_dnd(self):
        # Регистрируем виджет как цель для перетаскивания
        self.drop_target_register(DND_FILES)
        self.dnd_bind('<<DropEnter>>', self.on_drag_enter)
        self.dnd_bind('<<DropLeave>>', self.on_drag_leave)
        self.dnd_bind('<<Drop>>', self.on_drop)

    def bind_events(self):
        # Обработка кликов и наведения
        self.bind("<Button-1>", self.select_file)
        self.bind("<Enter>", lambda e: self.config(background="#e0e0e0"))
        self.bind("<Leave>", lambda e: self.config(background="#f0f0f0"))

    def on_drag_enter(self, event):
        self.config(background="#d0f0d0")  # Подсветка при перетаскивании

    def on_drag_leave(self, event):
        self.config(background="#f0f0f0")

    def on_drop(self, event):
        self.config(background="#e0f0e0")
        file_path = event.data.strip()

        # Обработка пути для Windows (удаление фигурных скобок)
        if file_path.startswith('{') and file_path.endswith('}'):
            file_path = file_path[1:-1]

        if os.path.exists(file_path):
            self.set_file(file_path)
        else:
            messagebox.showerror("Ошибка", "Файл не найден")
            self.config(background="#f0f0f0")

    def select_file(self, event=None):
        file_path = filedialog.askopenfilename(
            title="Выберите файл сметы",
            filetypes=(("Excel files", "*.xlsx"), ("All files", "*.*"))
        )
        if file_path:
            self.set_file(file_path)

    def set_file(self, file_path):
        try:
            if file_path is None:
                self.reset_widget()
                return

            path = os.path.abspath(os.path.normpath(file_path))
            if os.path.exists(path):
                self.file_path = path
                self.config(
                    text=f"Выбран файл:\n{os.path.basename(path)}",
                    background="#e0f0e0"
                )
            else:
                raise FileNotFoundError(f"Файл не существует: {path}")
        except Exception as e:
            self.reset_widget()
            messagebox.showerror("Ошибка", f"Неверный файл: {str(e)}")

    def reset_widget(self):
        self.file_path = None
        self.config(
            text="Перетащите файл сюда или нажмите для выбора",
            background="#f0f0f0"
        )

    def get_file(self):
        return self.file_path if (self.file_path and os.path.exists(self.file_path)) else None
