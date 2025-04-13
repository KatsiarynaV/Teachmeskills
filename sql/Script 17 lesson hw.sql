-- Задача 1: Анализ распределения мест в самолетах
-- Необходимо проанализировать распределение мест в самолетах по классам обслуживания. Рассчитать:
-- Общее количество мест в каждом самолете
-- Количество мест каждого класса
-- Процентное соотношение классов
-- Массив всех мест для каждого самолета

select * from bookings.seats;


select 
	aircraft_code,
	count(*) as seats_count,
	count(*) filter (where fare_conditions = 'Business') as business_seats,
	count(*) filter (where fare_conditions = 'Economy') as economy_seats,
	count(*) filter (where fare_conditions = 'Comfort') as comfort_seats,
	round(sum(case when fare_conditions = 'Business' then 1 else 0 end) * 100.0 / COUNT(*), 1) as business_percent,
	round(sum(case when fare_conditions = 'Economy' then 1 else 0 end) * 100.0 / COUNT(*), 1) as economy_percent,
	round(sum(case when fare_conditions = 'Comfort' then 1 else 0 end) * 100.0 / COUNT(*), 1) as comfort_percent,
	array_agg(seat_no order by seat_no) as seats_array
from bookings.seats
group by aircraft_code;



-- Задача 2: Анализ стоимости билетов по рейсам
-- Для каждого рейса рассчитать:
-- Минимальную, максимальную и среднюю стоимость билета
-- Разницу между самым дорогим и самым дешевым билетом
-- Медианную стоимость билета
-- Массив всех цен на билеты

select * from bookings.ticket_flights;

select 
	flight_id,
	min(amount) as min_amount,
	max(amount) as max_amount,
	round(avg(amount), 2) as avg_amount,
	max(amount) - min(amount) as amount_dif,
	percentile_cont(0.5) within group (order by amount) AS median_amount,
	array_agg(amount order by amount) as amount_array
from bookings.ticket_flights
group by flight_id;


-- Задача 3: Статистика по бронированиям по месяцам
-- Проанализировать бронирования по месяцам:
-- Количество бронирований
-- Общую сумму бронирований
-- Средний чек
-- Массив всех сумм бронирований для анализа распределения

select * from bookings.bookings;


select 
	extract(month from book_date) as book_month,
	count(*) as book_count,
	sum(total_amount) as book_total_amount,
	round(avg(total_amount), 2) as book_avg_amount,
	array_agg(total_amount order by total_amount) as total_amount_array
from bookings.bookings
group by book_month;


-- Задача 4: Анализ пассажиропотока по аэропортам
-- Рассчитать для каждого аэропорта:
-- Общее количество вылетов
-- Количество уникальных аэропортов назначения
-- Массив всех аэропортов назначения
-- Процент международных рейсов (если известны коды стран)

select * from bookings.flights;

select
	departure_airport,
	count(*) as flights_count,
	count(distinct arrival_airport) as unique_arrival_airport_count,
	array_agg(distinct arrival_airport order by arrival_airport) as arrival_airport_array
from bookings.flights
group by departure_airport;

