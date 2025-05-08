-- Задача 1 для самостоятельного задания: 
--Вам нужно проанализировать данные о продажах билетов, чтобы получить статистику в следующих разрезах:
-- По классам обслуживания (fare_conditions)
-- По месяцам вылета
-- По аэропортам вылета
-- По комбинациям: класс + месяц, класс + аэропорт, месяц + аэропорт
-- Общие итоги
--Используемые таблицы:
-- ticket_flights (информация о билетах)
-- flights (информация о рейсах)
-- airports (информация об аэропортах)

select 
	tf.fare_conditions,
	extract(month from f.scheduled_departure) as departure_month,
	a.airport_name,
	count(*) as total_count,
	round(avg(tf.amount), 2) as avg_amount
from bookings.ticket_flights tf
join bookings.flights f on f.flight_id = tf.flight_id
join bookings.airports a on a.airport_code = f.departure_airport
group by grouping sets (
	(tf.fare_conditions),
	(departure_month),
	(a.airport_name),
	(tf.fare_conditions, departure_month),
	(tf.fare_conditions, a.airport_name),
	(departure_month, a.airport_name),
	()
)
;

--Задача 2 для самостоятельного задания: 
--Рейсы с задержкой больше средней (CTE + подзапрос)
--Найдите рейсы, задержка которых превышает среднюю задержку по всем рейсам.

--Используемые таблицы:
-- flights

with average_delay as (
	select avg(extract(epoch from (actual_departure - scheduled_departure))) as avg_delay
	from bookings.flights f
	where actual_departure is not null
)
select
	flight_id,
	departure_airport,
	arrival_airport,
	extract(epoch from (actual_departure - scheduled_departure)) as delay
from bookings.flights f
where actual_departure is not null
and extract(epoch from (actual_departure - scheduled_departure)) > (select avg_delay from average_delay);


--Задача 3 для самостоятельного задания:
--Создайте представление, которое содержит все рейсы, вылетающие из Москвы.

create view bookings.flights_from_moscow as
select 
	flight_id,
	flight_no,
	scheduled_departure,
	scheduled_arrival,
	departure_airport,
	arrival_airport,
	status,
	aircraft_code
from bookings.flights
where departure_airport in (
	select airport_code
	from bookings.airports
	where city = 'Москва'
);


-- Изучить тему временных таблиц - сделать все возможные операции с ней (создать, заполнить, сджойнить, удалить) 
--результат исследования и скрипты прикрепить в домашке

-- 1. Создаём временную таблицу
create temp table temp_flights (
    flight_id serial primary key,
    departure_airport text not null,
    arrival_airport text not null,
    scheduled_departure timestamp not null,
    scheduled_arrival timestamp not null,
    delay_minutes integer default 0
);

-- 2. Заполняем временную таблицу 
insert into temp_flights (departure_airport, arrival_airport, scheduled_departure, scheduled_arrival, delay_minutes)
values
    ('SVO', 'LED', '2025-04-01 10:00:00', '2025-04-01 11:30:00', 15),
    ('DME', 'KZN', '2025-04-01 12:00:00', '2025-04-01 13:50:00', 5);

select * from temp_flights;

-- 3. Добавляем данные из другой таблицы
insert into temp_flights (departure_airport, arrival_airport, scheduled_departure, scheduled_arrival, delay_minutes)
select 
    departure_airport,
    arrival_airport,
    scheduled_departure,
    scheduled_arrival,
    extract(epoch from (actual_departure - scheduled_departure)) / 60 as delay_minutes
from bookings.flights
where departure_airport in ('SVO', 'DME');

-- 4. Выполняем JOIN с другой таблицей
select 
    tf.flight_id,
    tf.departure_airport,
    tf.arrival_airport,
    a.airport_name as departure_airport_name,
    tf.delay_minutes
from temp_flights tf
join bookings.airports a on tf.departure_airport = a.airport_code
where tf.delay_minutes > 10;

-- 5. Удаляем временную таблицу
drop table if exists temp_flights;



--Необходимо самостоятельно изучить триггерные функции, создать материализованное представление 
--для таблицы и настроить триггер, который будет срабатывать при обновлении или вставке данных в таблицу, 
--после чего выполнять обновление материализованного представления.

--Создание материализованного представления
create materialized view bookings.flights_summary AS
select 
    departure_airport,
    count(flight_id) as flight_count
from bookings.flights
group by departure_airport;

select * from bookings.flights_summary;

-- Создание триггерной функции
create or replace function update_flights_summary()
returns trigger as $$
begin
    refresh materialized view bookings.flights_summary;
    return null;
end;
$$ language plpgsql;

--Создание триггера
create trigger trigger_update_flights
after insert or update on bookings.flights
for each statement
execute function update_flights_summary();
















