#Задача. Реализовать даг в airflow, который генерирует 2 файла. 
# Состоять она должна из 4 тасок:
#1 таска. Пишет время начала генерации файлов
#2-3 таска. Параллельно 2 таски генерируют: одна файл xlsx в которой сохраняет 
# информацию сгенерированную по пользователям (можно рандомно через цикл 
# сгенерировать 10-20 записей). Пример:
#Василий,Смирнов,Минск
#и второй файл в формате txt в который вы запищите сгенерированные данные 
# рандомно сколько продаж было (тоже записей 30-80). Пример:
#Бананы,3шт,100р
#4 таска. Завершающая таска, которая открывает файлы сгенерированные и выводит 
# кол-во строчек в каждом из файлов

import pandas as pd
import random
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def generate_date():
    print(f"[START] Генерация файлов началась в {datetime.now()}")


def generate_xlsx_file(output_path):
    cities = ['Минск', 'Москва', 'Париж', 'Лондон', 'Нью-Йорк', 'Берлин', 'Сидней', 'Гродно']
    first_names = ['Алексей', 'Мария', 'Иван', 'Ольга', 'Дмитрий', 'Елена', 'Наталья', 'Анна']
    last_names = ['Иванов', 'Петров', 'Сидоров', 'Смирнов', 'Кузнецов', 'Виноградов', 'Семенов', 'Дмитриев']    
    data = []
    for i in range(random.randint(10, 20)):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        city = random.choice(cities)
        data.append([first_name, last_name, city])
    df = pd.DataFrame(data, columns=['Имя', 'Фамилия', 'Город'])
    df.to_excel(output_path, index=False)
    print(f"[v] Данные сгенерированы")


def generate_txt_file(output_path):
    items = ['Бананы', 'Яблоки', 'Молоко', 'Хлеб', 'Мясо', 'Апельсины', 'Сыр', 'Рыба', 'Печенье']
    with open(output_path, "w", encoding="utf-8") as f:
        for _ in range(random.randint(30, 80)):
            item = random.choice(items)
            quantity = random.randint(1, 10)
            price = random.randint(50, 500)
            f.write(f"{item},{quantity}шт,{price}р\n")
        print(f"[v] Данные сгенерированы")


def count_files_lines(xlsx_path, txt_path):
    try:
        xlsx_df = pd.read_excel(xlsx_path)
        print(f"[v] В файле users.xlsx: {len(xlsx_df)} строк")
    except Exception as e:
        print(f"[x] Ошибка чтения Excel: {e}")
    try:
        with open(txt_path, encoding="utf-8") as f:
            txt_lines = sum(1 for _ in f)
        print(f"[v] В файле sales.txt: {txt_lines} строк")
    except Exception as e:
        print(f"[x] Ошибка чтения TXT: {e}")


with DAG(
    dag_id='files_generation',
    description='Даг, который генерирует файлы',
    schedule_interval='0 0 12 * *',
    start_date=datetime(2025, 6, 11),
    catchup=False,
) as dag:
    
    task_start = PythonOperator(
        task_id='generation_date',
        python_callable=generate_date
    )

    task_xlsx = PythonOperator(
        task_id='generation_xlsx',
        python_callable=generate_xlsx_file,
        op_kwargs={'output_path': '/opt/airflow/dags/data/users.xlsx'}
    )

    task_txt = PythonOperator(
        task_id='generation_txt',
        python_callable=generate_txt_file,
        op_kwargs={'output_path': '/opt/airflow/dags/data/sales.txt'}
    )

    task_count = PythonOperator(
        task_id='count_lines',
        python_callable=count_files_lines,
        op_kwargs={
        'xlsx_path': '/opt/airflow/dags/data/users.xlsx',
        'txt_path': '/opt/airflow/dags/data/sales.txt'
        }
    )


    task_start >> [task_xlsx, task_txt] >> task_count