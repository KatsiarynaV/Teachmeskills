# Задача 1. Экспорт расписания рейсов по конкретному маршруту.
# Нужно создать функцию на Python, которая выгружает в CSV-файл расписание рейсов между двумя городами (например, Москва и Санкт-Петербург). Функция должна включать:
# - Номер рейса
# - Время вылета и прилета
# - Тип самолета
# - Среднюю цену билета
# ❗️SELECT сделать без использования pandas!❗️

import psycopg2
from dotenv import load_dotenv
import os
import csv

def get_connection():
    load_dotenv()
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def export_flights_by_routes(departure_city: str, arrival_city: str, file_path, conn):
    query = f"""
        select 
            a2.airport_name as departure_city,
            a3.airport_name as arrival_city,
            f.flight_id,
            f.flight_no,
            f.scheduled_departure,
            f.scheduled_arrival,
            a.model,
            round(avg(tf.amount), 2) as avg_amount
        from bookings.flights f
        join bookings.aircrafts a on a.aircraft_code =f.aircraft_code
        join bookings.ticket_flights tf on tf.flight_id = f.flight_id
        join bookings.airports a2 on a2.airport_code = f.departure_airport 
        join bookings.airports a3 on a3.airport_code = f.arrival_airport
        WHERE a2.airport_name = %s
        and a3.airport_name = %s
        group by f.flight_id, f.flight_no, f.scheduled_departure, f.scheduled_arrival, a.model, a2.airport_name, a3.airport_name;          
    """

    with conn.cursor() as cur:
        cur.execute(query, (departure_city, arrival_city))
        rows = cur.fetchall()
        column_names = [desc[0] for desc in cur.description]
    with open(file_path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(column_names)  # Заголовки
        writer.writerows(rows)         # Данные
    print(f"Данные успешно экспортированы в {file_path}")

# Задача 2. Массовое обновление статусов рейсов
# Создать функцию для пакетного обновления статусов рейсов (например, "Задержан" или "Отменен"). Функция должна:
# - Принимать список рейсов и их новых статусов
# - Подтверждать количество обновленных записей
# - Обрабатывать ошибки (например, несуществующие рейсы)
# Пример входных данных:
# updates = [
#     {"flight_id": 123, "new_status": "Delayed"},
#     {"flight_id": 456, "new_status": "Cancelled"}
# ]
# update_flights_status(updates)

def update_flights_status(updates: list):
    query = """
        update bookings.flights
        set status = %s
        where flight_id = %s;
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        updates_tuples = [(update["new_status"], update["flight_id"]) for update in updates]
        cursor.executemany(query,updates_tuples)
        conn.commit()
        if cursor.rowcount == 0:
            print("Ни одна запись не была обновлена. Проверьте корректность ID рейсов.")
        else:
            print(f"Обновлено записей: {cursor.rowcount}")
            print("Данные успешно обновлены")
    except Exception as e:
        print(f"Ошибка при обновлении статусов рейсов: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    export_flights_by_routes('Домодедово', 'Надым', 'flights_by_routes.csv', get_connection())
    updates = [
        {"flight_id": 123, "new_status": "Delayed"},
        {"flight_id": 456, "new_status": "Cancelled"}
    ]
    update_flights_status(updates)