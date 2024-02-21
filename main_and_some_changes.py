from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
import os
import xml.etree.ElementTree as ET
import csv
from multiprocessing import Manager

CHUNK_SIZE = 1
ROWS_LEN_TO_WRITE = 100


def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        print(f"Start time: {start_time}")
        result = func(*args, **kwargs)
        end_time = datetime.now()
        print(f"End time: {end_time}")
        print(f"Time spent: {end_time - start_time}")
        return result

    return wrapper


def process_and_write_xml(xml_files, output_file, shared_fieldnames, lock):
    print(f'Processing files: {xml_files}...')
    with open(output_file, 'a', newline='') as csv_file:
        writer = csv.writer(csv_file)

        # Проверяем, нужно ли записать заголовки
        with lock:
            if not shared_fieldnames:
                shared_fieldnames.extend(['ProductName', 'Quantity', 'Price', 'Comment'])
                writer.writerow(shared_fieldnames)

        local_rows = []

        for xml_file in xml_files:
            try:
                tree = ET.parse(xml_file)
                root = tree.getroot()

                for item in root.findall('.//Item'):
                    product_name = item.find('ProductName')
                    quantity = item.find('Quantity')
                    price = item.find('Price')
                    comment = item.find('Comment')

                    if product_name is not None and quantity is not None and price is not None and comment is not None:
                        row = [product_name.text, quantity.text, price.text, comment.text]
                        local_rows.append(row)

                        if len(local_rows) >= ROWS_LEN_TO_WRITE:
                            # Если достигнут размер ROWS_LEN_TO_WRITE, записываем его в CSV с использованием мьютекса
                            with lock:
                                writer.writerows(local_rows)
                                local_rows = []

            except Exception as e:
                print(f"Error processing {xml_file}: {e}")

        # Записываем оставшиеся строки в CSV
        with lock:
            writer.writerows(local_rows)


@timing_decorator
def main():
    input_folder = "./xml_files/"
    output_file = "./csv_files/new_csv_file.csv"

    manager = Manager()
    shared_fieldnames = manager.list()
    lock = manager.Lock()  # Используем Lock из Manager

    xml_all_files = [os.path.join(input_folder, file) for file in os.listdir(input_folder) if file.endswith(".xml")]

    pool_size = os.cpu_count()

    with ProcessPoolExecutor(max_workers=pool_size) as executor:
        # Разбиваем список файлов на подсписки для обработки
        xml_files = [xml_all_files[i:i + CHUNK_SIZE] for i in range(0, len(xml_all_files), CHUNK_SIZE)]

        # Запускаем процессы на обработку файлов и запись результатов
        executor.map(process_and_write_xml, xml_files, [output_file] * len(xml_files),
                     [shared_fieldnames] * len(xml_files), [lock] * len(xml_files))


if __name__ == "__main__":
    main()
