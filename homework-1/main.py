"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv
from pathlib import Path

# Унифицируем пути до CSV-файлов
ROOT = Path(__file__).resolve().parent
NORTH_DATA_PATH = Path.joinpath(ROOT, 'north_data')

#  Список файлов, из которых импортируем данные в базу данных
csv_filenames = ['customers_data.csv', 'employees_data.csv', 'orders_data.csv']


def main():

    try:   # Устанавливаем соединение с базой данных
        connection = psycopg2.connect(host="localhost", database="north", user="postgres", password="15679")
    except psycopg2.OperationalError as err:
        print('Ошибка подключения к базе данных:', err)
        return

    try:
        with connection:
            with connection.cursor() as cur:
                # Импортируем данные в цикле из всех файлов, указанных в списке csv_filenames
                for filename in csv_filenames:
                    with open(Path.joinpath(NORTH_DATA_PATH, filename)) as data_file:
                        csv_reader = csv.reader(data_file)
                        # Вычисляем количество столбцов в CSV-файле. Заодно пропускаем первую строку с заголовками
                        num_columns: int = len(next(csv_reader))
                        # Получаем наименование таблицы взяв его из имени CSV-файла:
                        # от первого символа имени файла и заканчивая символом подчеркивания
                        table_name: str = filename[:filename.find('_')]
                        # Формируем универсальную строку SQL-запроса для записи данных в таблицу БД
                        insert_query: str = f'INSERT INTO {table_name} VALUES ({",".join(["%s"] * num_columns)})'
                        # Построчно берем данные из csv-файла и с помощью строки запроса заполняем таблицы БД
                        for row in csv_reader:
                            cur.execute(insert_query, row)
    except (psycopg2.DatabaseError, psycopg2.DataError, psycopg2.IntegrityError) as err:
        print('Ошибка записи в базу данных:', err)
    except FileNotFoundError:
        print(f'Не найден файл {filename}')
    finally:   # Если при работе программы возникло исключение, а соединение с БД осталось открытым, то закрываем его.
        connection.close()   # Если исключений не возникло, то менеджер контекста with сам закроет соединение


if __name__ == '__main__':
    main()
