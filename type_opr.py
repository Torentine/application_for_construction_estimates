import struct
import os


def get_file_signature(file_path, num_bytes=8):
    """Читает первые num_bytes байтов файла."""
    with open(file_path, 'rb') as f:
        return f.read(num_bytes)


def identify_file_type(file_path):
    """Определяет тип файла по сигнатуре."""
    signatures = {
        b'<?xml ': "XML",
        b'PK\x03\x04': "XLSX",  # ZIP-архив, так как XLSX — это zip
        b'GGE ': "GGE",
        b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1': "XLS"  # Сигнатура OLE2 Compound File (старый формат XLS)
    }

    file_signature = get_file_signature(file_path, max(len(sig) for sig in signatures))

    for sig, file_type in signatures.items():
        if file_signature.startswith(sig):
            if file_type == "XML":
                file_extension = os.path.splitext(file_path)[1].lower()
                if file_extension == ".gge":
                    return "GGE"
            return file_type

    return "Неизвестный формат"


if __name__ == "__main__":
    file_path = input("Введите путь к файлу: ")
    file_type = identify_file_type(file_path)
    print(f"Файл {file_path} имеет тип: {file_type}")
