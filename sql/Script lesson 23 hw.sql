--Задача 1:
--Необходимо оптимизировать выборку данных по номеру места (bookings.boarding_passes.seat_no) 
--с помощью индекса и сравнить результаты до добавления индекса и после (время выполнения и объем таблицы в %)

explain analyze --8000-9000 ms, 1,1 Gb без индекса; 2000-2500 ms (~ 73,5%), 1,1 Gb с индексом
select 
	*
from bookings.boarding_passes
where seat_no = '13A'
;

create index idx_boarding_passes_seat_no on bookings.boarding_passes (seat_no);
drop index bookings.idx_boarding_passes_seat_no;



--Задача 2:
--1. Проанализировать производительность запроса без индексов.
--2. Добавить индексы для ускорения JOIN и фильтрации.
--3. Снова проанализировать производительность запроса и сравнить результаты.
--SELECT bp.boarding_no, t.passenger_id
--FROM bookings.boarding_passes bp
--JOIN bookings.tickets t ON bp.ticket_no = t.ticket_no
--JOIN bookings.seats s ON bp.seat_no = s.seat_no
--JOIN bookings.bookings b ON t.book_ref = b.book_ref
--WHERE 
 -- t.passenger_id in ('0856 579180', '4723 695013')
 -- and boarding_no < 100
--;

explain analyze    --1000-1500 ms без индекса; 0.1-0.2 ms (~ 99,9%) с индексом
SELECT bp.boarding_no, t.passenger_id
FROM bookings.boarding_passes bp
JOIN bookings.tickets t ON bp.ticket_no = t.ticket_no
JOIN bookings.seats s ON bp.seat_no = s.seat_no
JOIN bookings.bookings b ON t.book_ref = b.book_ref
WHERE 
  t.passenger_id in ('0856 579180', '4723 695013')
  and boarding_no < 100
;

create index idx_tickets_ticket_no on bookings.tickets (ticket_no);
create index idx_seats_seat_no on bookings.seats (seat_no);
create index idx_bookings_book_ref on bookings.bookings (book_ref);
create index idx_boarding_passes_boarding_no on bookings.boarding_passes (boarding_no);
create index idx_tickets_passenger_id on bookings.tickets (passenger_id);

drop index bookings.idx_boarding_passes_boarding_no;
drop index bookings.idx_tickets_passenger_id;
drop index bookings.idx_tickets_ticket_no;
drop index bookings.idx_seats_seat_no;
drop index bookings.idx_bookings_book_ref;
