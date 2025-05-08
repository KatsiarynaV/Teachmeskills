--Задание 3.1: Анализ задержек рейсов по аэропорту
--Задача: Для указанного аэропорта (по коду аэропорта) вывести статистику задержек.
--Таблица: flights

select 
	departure_airport,
	min(extract(epoch from (actual_departure - scheduled_departure)))             as min_delay,
	max(extract(epoch from (actual_departure - scheduled_departure)))             as max_delay,
	round(avg(extract(epoch from (actual_departure - scheduled_departure))), 2)   as avg_delay
from bookings.flights
where departure_airport = 'DME'
group by departure_airport;
	

--Задание 3.2: Обернуть в функцию c вводом кода аэропорта

create or replace function bookings.get_airport_delay_stat(airport_code bpchar)
returns table (
	departure_airport bpchar,
	min_delay numeric,
	max_delay numeric,
	avg_delay numeric
) as $$
begin
	return query
	select 
		f.departure_airport,
		min(extract(epoch from (f.actual_departure - f.scheduled_departure)))            as min_delay,
		max(extract(epoch from (f.actual_departure - f.scheduled_departure)))            as max_delay,
		round(avg(extract(epoch from (f.actual_departure - f.scheduled_departure))),2)   as avg_delay
	from bookings.flights f
	where f.departure_airport = get_airport_delay_stat.airport_code
	group by f.departure_airport;

end;
$$ language plpgsql;


select * from bookings.get_airport_delay_stat('DME');


--Задание 5.1: Маршрутная сеть с пересадками
--Задача: Найти все возможные маршруты из указанного аэропорта с 1 пересадкой.
--Таблица: flights
--Пример: Перелет из Абакан в Анадырь через Шереметьево (SVO), таблица:
--1 connection  Абакан
--Direct  Анадырь


select distinct
    f1.departure_airport  as origin_airport,
    f1.arrival_airport    as connection_airport,
    f2.arrival_airport    as destination_airport,
    a1.airport_name       as origin_airport_name,
    a2.airport_name       as connection_airport_name,
    a3.airport_name       as destination_airport_name
from bookings.flights f1
join bookings.flights f2 on f1.arrival_airport = f2.departure_airport
join bookings.airports a1 on a1.airport_code = f1.departure_airport 
join bookings.airports a2 on a2.airport_code = f1.arrival_airport 
join bookings.airports a3 on a3.airport_code = f2.arrival_airport 
where f1.departure_airport = 'ABA' 
and f2.arrival_airport != f1.departure_airport
;


--Задание 5.2: Создать функцию с параметром аэропорта

create or replace function bookings.get_routes_with_stopover(airport_code bpchar)
returns table (
    origin_airport bpchar,
    connection_airport bpchar,
    destination_airport bpchar,
    origin_airport_name text,
    connection_airport_name text,
    destination_airport_name text   
) as $$
begin
	return query
	select distinct
	    f1.departure_airport  as origin_airport,
	    f1.arrival_airport    as connection_airport,
	    f2.arrival_airport    as destination_airport,
	    a1.airport_name       as origin_airport_name,
	    a2.airport_name       as connection_airport_name,
	    a3.airport_name       as destination_airport_name
	from bookings.flights f1
	join bookings.flights f2 on f1.arrival_airport = f2.departure_airport
	join bookings.airports a1 on a1.airport_code = f1.departure_airport 
	join bookings.airports a2 on a2.airport_code = f1.arrival_airport 
	join bookings.airports a3 on a3.airport_code = f2.arrival_airport 
	where f1.departure_airport = get_routes_with_stopover.airport_code 
	and f2.arrival_airport != f1.departure_airport;
end;
$$ language plpgsql;


select * from bookings.get_routes_with_stopover('ABA');
