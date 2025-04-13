-- Задача 1: Вывести аэропорты, из которых выполняется менее 50 рейсов

select 
	a.airport_name,
	f.departure_airport,
	count(f.flight_no) as flights_count
from bookings.airports a 
inner join bookings.flights f on f.departure_airport = a.airport_code
group by f.departure_airport, a.airport_name
having count(f.flight_no) < 150 -- для запроса < 50 нет результатов в большой базе данных
;


-- Задача 2: Вывести среднюю стоимость билетов для каждого маршрута (город вылета - город прилета)

select 
	a.city as departure_city,
	a2.city as arrival_city,
	round(avg(tf.amount), 2) as avg_amount
from bookings.ticket_flights tf 
inner join bookings.flights f on f.flight_id = tf.flight_id
inner join bookings.airports a on a.airport_code = f.departure_airport
inner join bookings.airports a2 on a2.airport_code = f.arrival_airport
group by departure_city, arrival_city;



-- Задача 3: Вывести топ-5 самых загруженных маршрутов (по количеству проданных билетов)

select 
	a.city as departure_city,
	a2.city as arrival_city,
	count(tf.ticket_no) as tickets_sold
from bookings.ticket_flights tf  
inner join bookings.flights f on f.flight_id = tf.flight_id
inner join bookings.airports a on a.airport_code = f.departure_airport
inner join bookings.airports a2 on a2.airport_code = f.arrival_airport
group by departure_city, arrival_city
order by tickets_sold desc
limit 5;






