--Задача 1: Анализ частоты полетов пассажиров
--Определить топ-10 пассажиров, которые чаще всего летают.
--Провести оптимизацию скрипта по необходимости

explain analyze
select 
	t.passenger_id,
	count(tf.flight_id) as flights_count
from bookings.tickets t 
join bookings.ticket_flights tf on tf.ticket_no = t.ticket_no
group by t.passenger_id
order by flights_count desc
limit 10;

-- Результат без индексов

--Limit  (cost=142472.10..142472.13 rows=10 width=20) (actual time=2046.075..2046.097 rows=10 loops=1)
--  ->  Sort  (cost=142472.10..143388.94 rows=366733 width=20) (actual time=2038.275..2038.294 rows=10 loops=1)
--        Sort Key: (count(tf.flight_id)) DESC
--        Sort Method: top-N heapsort  Memory: 26kB
--        ->  HashAggregate  (cost=120667.64..134547.14 rows=366733 width=20) (actual time=1660.920..1983.089 rows=366733 loops=1)
--              Group Key: t.passenger_id
--              Planned Partitions: 8  Batches: 9  Memory Usage: 8273kB  Disk Usage: 31840kB
--              ->  Hash Join  (cost=16934.49..53675.82 rows=1045726 width=16) (actual time=166.833..1221.105 rows=1045726 loops=1)
--                    Hash Cond: (tf.ticket_no = t.ticket_no)
--                    ->  Seq Scan on ticket_flights tf  (cost=0.00..19233.26 rows=1045726 width=18) (actual time=0.099..323.386 rows=1045726 loops=1)
--                    ->  Hash  (cost=9843.33..9843.33 rows=366733 width=26) (actual time=166.474..166.481 rows=366733 loops=1)
--                         Buckets: 131072  Batches: 4  Memory Usage: 6244kB
--                          ->  Seq Scan on tickets t  (cost=0.00..9843.33 rows=366733 width=26) (actual time=0.009..62.778 rows=366733 loops=1)
--Planning Time: 0.270 ms
--JIT:
--  Functions: 18
--  Options: Inlining false, Optimization false, Expressions true, Deforming true
--  Timing: Generation 0.956 ms, Inlining 0.000 ms, Optimization 0.830 ms, Emission 11.316 ms, Total 13.103 ms
--Execution Time: 2051.956 ms

-- Результат с индексами - все равно использует Seq Scan

--Limit  (cost=142472.10..142472.13 rows=10 width=20) (actual time=2032.790..2032.796 rows=10 loops=1)
--  ->  Sort  (cost=142472.10..143388.94 rows=366733 width=20) (actual time=2003.106..2003.110 rows=10 loops=1)
--        Sort Key: (count(tf.flight_id)) DESC
--        Sort Method: top-N heapsort  Memory: 26kB
--        ->  HashAggregate  (cost=120667.64..134547.14 rows=366733 width=20) (actual time=1605.888..1946.986 rows=366733 loops=1)
--             Group Key: t.passenger_id
--              Planned Partitions: 8  Batches: 9  Memory Usage: 8273kB  Disk Usage: 31840kB
--              ->  Hash Join  (cost=16934.49..53675.82 rows=1045726 width=16) (actual time=159.964..1179.219 rows=1045726 loops=1)
--                   Hash Cond: (tf.ticket_no = t.ticket_no)
--                    ->  Seq Scan on ticket_flights tf  (cost=0.00..19233.26 rows=1045726 width=18) (actual time=0.048..298.533 rows=1045726 loops=1)
--                    ->  Hash  (cost=9843.33..9843.33 rows=366733 width=26) (actual time=159.744..159.745 rows=366733 loops=1)
--                          Buckets: 131072  Batches: 4  Memory Usage: 6244kB
--                          ->  Seq Scan on tickets t  (cost=0.00..9843.33 rows=366733 width=26) (actual time=0.012..58.927 rows=366733 loops=1)
--Planning Time: 9.863 ms
--JIT:
-- Functions: 18
-- Options: Inlining false, Optimization false, Expressions true, Deforming true
--  Timing: Generation 10.035 ms, Inlining 0.000 ms, Optimization 7.878 ms, Emission 25.620 ms, Total 43.532 ms
--Execution Time: 2052.710 ms

create index idx_tickets_ticket_no on bookings.tickets (ticket_no);
create index idx_ticket_flights_ticket_no on bookings.ticket_flights (ticket_no);
create index idx_tickets_passenger_id on bookings.tickets (passenger_id);

drop index bookings.idx_tickets_ticket_no;
drop index bookings.idx_ticket_flights_ticket_no;
drop index bookings.idx_tickets_passenger_id;

-- если сделать принудительно использование индексов, то время обработки остается такое же
set enable_seqscan = off;

--Limit  (cost=156795.22..156795.25 rows=10 width=20) (actual time=1626.976..1626.980 rows=10 loops=1)
--  ->  Sort  (cost=156795.22..157712.06 rows=366733 width=20) (actual time=1622.360..1622.363 rows=10 loops=1)
--        Sort Key: (count(tf.flight_id)) DESC
--        Sort Method: top-N heapsort  Memory: 26kB
--        ->  HashAggregate  (cost=134990.76..148870.26 rows=366733 width=20) (actual time=1166.310..1561.105 rows=366733 loops=1)
--              Group Key: t.passenger_id
--              Planned Partitions: 8  Batches: 9  Memory Usage: 8273kB  Disk Usage: 31968kB
--              ->  Merge Join  (cost=0.85..67998.94 rows=1045726 width=16) (actual time=0.049..806.545 rows=1045726 loops=1)
--                    Merge Cond: (t.ticket_no = tf.ticket_no)
--                    ->  Index Scan using idx_tickets_ticket_no on tickets t  (cost=0.42..17340.42 rows=366733 width=26) (actual time=0.019..79.945 rows=366733 loops=1)
--                    ->  Index Only Scan using ticket_flights_pkey on ticket_flights tf  (cost=0.42..36670.11 rows=1045726 width=18) (actual time=0.014..141.169 rows=1045726 loops=1)
--                          Heap Fetches: 0
--Planning Time: 0.301 ms
--JIT:
--  Functions: 13
--  Options: Inlining false, Optimization false, Expressions true, Deforming true
--  Timing: Generation 0.721 ms, Inlining 0.000 ms, Optimization 0.534 ms, Emission 7.283 ms, Total 8.537 ms
--Execution Time: 2105.269 ms


--Задача 2 (усложнение задачи 5 из самостоятельного решения). Анализ загрузки самолетов по дням недели
--Определить, в какие дни недели самолеты загружены больше всего.

--Логика расчета. Шаги:
-- Используем данные о занятых местах (boarding_passes) и общем количестве мест (seats).
-- Группируем данные по дням недели.
-- Рассчитаем среднюю загрузку самолетов для каждого дня.

explain analyze
with total_seats as (
    select aircraft_code, count(*) as total_seats
    from bookings.seats
    group by aircraft_code
),
occupied_seats as (
    select 
        tf.flight_id,
        count(distinct bp.seat_no) as occupied_seats
    from bookings.boarding_passes bp
    join bookings.ticket_flights tf on bp.ticket_no = tf.ticket_no
    group by tf.flight_id
),
flight_load as (
    select
        f.flight_id,
        f.scheduled_departure,
        ts.total_seats,
        coalesce(os.occupied_seats, 0) as occupied_seats,
        round((coalesce(os.occupied_seats, 0)::numeric / ts.total_seats) * 100, 2) as load_percentage
    from bookings.flights f
    join total_seats ts on f.aircraft_code = ts.aircraft_code
    left join occupied_seats os on f.flight_id = os.flight_id
),
weekly_avg_load as (
    select 
        extract(dow from scheduled_departure) as day_of_week,
        round(avg(load_percentage), 2) as avg_load_percentage
    from flight_load
    group by extract(dow from scheduled_departure)
)
select * from weekly_avg_load
order by day_of_week;


-- Результат без индексов

--GroupAggregate  (cost=323717.74..324558.89 rows=10213 width=64) (actual time=5221.115..5242.117 rows=7 loops=1)
--  Group Key: (EXTRACT(dow FROM f.scheduled_departure))
--  ->  Sort  (cost=323717.74..323800.54 rows=33121 width=48) (actual time=5216.592..5219.533 rows=33121 loops=1)
--        Sort Key: (EXTRACT(dow FROM f.scheduled_departure))
--        Sort Method: quicksort  Memory: 2867kB
--        ->  Hash Join  (cost=306428.42..321231.10 rows=33121 width=48) (actual time=4260.707..5203.216 rows=33121 loops=1)
--              Hash Cond: (f.aircraft_code = ts.aircraft_code)
--              ->  Hash Right Join  (cost=306400.04..320992.04 rows=33121 width=20) (actual time=4212.934..5132.341 rows=33121 loops=1)
--                    Hash Cond: (tf.flight_id = f.flight_id)
--                    ->  GroupAggregate  (cost=305262.82..319663.60 rows=15146 width=12) (actual time=4108.891..5015.775 rows=15881 loops=1)
--                          Group Key: tf.flight_id
--                          ->  Sort  (cost=305262.82..310012.59 rows=1899909 width=7) (actual time=4108.767..4592.420 rows=1906184 loops=1)
--                                Sort Key: tf.flight_id, bp.seat_no
--                                Sort Method: external merge  Disk: 32960kB
--                               ->  Hash Join  (cost=20727.94..81149.07 rows=1899909 width=7) (actual time=362.711..1714.238 rows=1906184 loops=1)
--                                      Hash Cond: (tf.ticket_no = bp.ticket_no)
--                                      ->  Seq Scan on ticket_flights tf  (cost=0.00..19233.26 rows=1045726 width=18) (actual time=0.077..350.813 rows=1045726 loops=1)
--                                      ->  Hash  (cost=10084.86..10084.86 rows=579686 width=17) (actual time=362.485..362.486 rows=579686 loops=1)
--                                            Buckets: 131072  Batches: 8  Memory Usage: 4531kB
--                                            ->  Seq Scan on boarding_passes bp  (cost=0.00..10084.86 rows=579686 width=17) (actual time=0.827..198.400 rows=579686 loops=1)
--                    ->  Hash  (cost=723.21..723.21 rows=33121 width=16) (actual time=103.964..103.964 rows=33121 loops=1)
--                          Buckets: 65536  Batches: 1  Memory Usage: 2065kB
--                          ->  Seq Scan on flights f  (cost=0.00..723.21 rows=33121 width=16) (actual time=1.390..93.588 rows=33121 loops=1)
--              ->  Hash  (cost=28.27..28.27 rows=9 width=12) (actual time=45.009..45.011 rows=9 loops=1)
--                    Buckets: 1024  Batches: 1  Memory Usage: 9kB
--                    ->  Subquery Scan on ts  (cost=28.09..28.27 rows=9 width=12) (actual time=44.998..45.002 rows=9 loops=1)
--                          ->  HashAggregate  (cost=28.09..28.18 rows=9 width=12) (actual time=44.993..44.995 rows=9 loops=1)
--                                Group Key: seats.aircraft_code
--                                Batches: 1  Memory Usage: 24kB
--                               ->  Seq Scan on seats  (cost=0.00..21.39 rows=1339 width=4) (actual time=0.458..1.868 rows=1339 loops=1)
--Planning Time: 9.486 ms
--JIT:
--  Functions: 40
--  Options: Inlining false, Optimization false, Expressions true, Deforming true
--  Timing: Generation 2.915 ms, Inlining 0.000 ms, Optimization 3.242 ms, Emission 38.710 ms, Total 44.867 ms
--Execution Time: 5252.373 ms

create index idx_ticket_flights_ticket_no_flight_id
  on bookings.ticket_flights (ticket_no, flight_id);
create index idx_boarding_passes_ticket_no_seat_no
  on bookings.boarding_passes (ticket_no, seat_no);
create index idx_flights_aircraft_code_flight_id_scheduled_departure
  on bookings.flights (aircraft_code, flight_id, scheduled_departure);
create index idx_seats_aircraft_code
  on bookings.seats (aircraft_code);

drop index bookings.idx_ticket_flights_ticket_no_flight_id;
drop index bookings.idx_boarding_passes_ticket_no_seat_no;
drop index bookings.idx_flights_aircraft_code_flight_id_scheduled_departure;
drop index bookings.idx_seats_aircraft_code;


-- Результат с индексами - также все равно использует Seq Scan

--GroupAggregate  (cost=323717.74..324558.89 rows=10213 width=64) (actual time=5157.366..5177.831 rows=7 loops=1)
--  Group Key: (EXTRACT(dow FROM f.scheduled_departure))
--  ->  Sort  (cost=323717.74..323800.54 rows=33121 width=48) (actual time=5154.855..5157.420 rows=33121 loops=1)
--        Sort Key: (EXTRACT(dow FROM f.scheduled_departure))
--        Sort Method: quicksort  Memory: 2867kB
--        ->  Hash Join  (cost=306428.42..321231.10 rows=33121 width=48) (actual time=4268.876..5141.997 rows=33121 loops=1)
--              Hash Cond: (f.aircraft_code = ts.aircraft_code)
--              ->  Hash Right Join  (cost=306400.04..320992.04 rows=33121 width=20) (actual time=4115.035..4968.431 rows=33121 loops=1)
--                    Hash Cond: (tf.flight_id = f.flight_id)
--                    ->  GroupAggregate  (cost=305262.82..319663.60 rows=15146 width=12) (actual time=4098.767..4941.386 rows=15881 loops=1)
--                          Group Key: tf.flight_id
--                          ->  Sort  (cost=305262.82..310012.59 rows=1899909 width=7) (actual time=4098.644..4545.748 rows=1906184 loops=1)
--                                Sort Key: tf.flight_id, bp.seat_no
--                                Sort Method: external merge  Disk: 32960kB
--                                ->  Hash Join  (cost=20727.94..81149.07 rows=1899909 width=7) (actual time=273.974..1666.210 rows=1906184 loops=1)
--                                      Hash Cond: (tf.ticket_no = bp.ticket_no)
--                                      ->  Seq Scan on ticket_flights tf  (cost=0.00..19233.26 rows=1045726 width=18) (actual time=0.553..378.334 rows=1045726 loops=1)
--                                      ->  Hash  (cost=10084.86..10084.86 rows=579686 width=17) (actual time=272.158..272.159 rows=579686 loops=1)
--                                            Buckets: 131072  Batches: 8  Memory Usage: 4531kB
--                                            ->  Seq Scan on boarding_passes bp  (cost=0.00..10084.86 rows=579686 width=17) (actual time=0.340..107.088 rows=579686 loops=1)
--                    ->  Hash  (cost=723.21..723.21 rows=33121 width=16) (actual time=16.208..16.210 rows=33121 loops=1)
--                          Buckets: 65536  Batches: 1  Memory Usage: 2065kB
--                          ->  Seq Scan on flights f  (cost=0.00..723.21 rows=33121 width=16) (actual time=0.027..7.865 rows=33121 loops=1)
--              ->  Hash  (cost=28.27..28.27 rows=9 width=12) (actual time=149.996..149.997 rows=9 loops=1)
--                    Buckets: 1024  Batches: 1  Memory Usage: 9kB
--                    ->  Subquery Scan on ts  (cost=28.09..28.27 rows=9 width=12) (actual time=149.961..149.966 rows=9 loops=1)
--                          ->  HashAggregate  (cost=28.09..28.18 rows=9 width=12) (actual time=149.955..149.958 rows=9 loops=1)
--                                Group Key: seats.aircraft_code
--                                Batches: 1  Memory Usage: 24kB
--                                ->  Seq Scan on seats  (cost=0.00..21.39 rows=1339 width=4) (actual time=0.040..0.181 rows=1339 loops=1)
--Planning Time: 14.583 ms
--JIT:
--  Functions: 40
--  Options: Inlining false, Optimization false, Expressions true, Deforming true
--  Timing: Generation 12.501 ms, Inlining 0.000 ms, Optimization 25.744 ms, Emission 123.846 ms, Total 162.090 ms
--Execution Time: 5198.433 ms


set enable_seqscan = off;

-- и также если сделать принудительно использование индексов, то время обработки остается такое же
--        Sort Method: quicksort  Memory: 2867kB
--        ->  Hash Join  (cost=312445.85..327248.53 rows=33121 width=48) (actual time=4986.882..5747.140 rows=33121 loops=1)
--              Hash Cond: (f.aircraft_code = ts.aircraft_code)
--              ->  Hash Right Join  (cost=312402.50..326994.49 rows=33121 width=20) (actual time=4957.863..5698.192 rows=33121 loops=1)
--                    Hash Cond: (tf.flight_id = f.flight_id)
--                    ->  GroupAggregate  (cost=310975.38..325376.16 rows=15146 width=12) (actual time=4939.875..5669.623 rows=15881 loops=1)
--                          Group Key: tf.flight_id
--                          ->  Sort  (cost=310975.38..315725.15 rows=1899909 width=7) (actual time=4939.772..5274.547 rows=1906184 loops=1)
--                                Sort Key: tf.flight_id, bp.seat_no
--                                Sort Method: external merge  Disk: 32984kB
--                                ->  Merge Join  (cost=0.85..86861.63 rows=1899909 width=7) (actual time=0.068..2323.767 rows=1906184 loops=1)
--                                      Merge Cond: (bp.ticket_no = tf.ticket_no)
--                                      ->  Index Only Scan using idx_boarding_passes_ticket_no_seat_no on boarding_passes bp  (cost=0.42..20243.71 rows=579686 width=17) (actual time=0.016..339.409 rows=579686 loops=1)
--                                            Heap Fetches: 0
--                                      ->  Materialize  (cost=0.42..39284.43 rows=1045726 width=18) (actual time=0.013..1045.499 rows=2270945 loops=1)
--                                            ->  Index Only Scan using idx_ticket_flights_ticket_no_flight_id on ticket_flights tf  (cost=0.42..36670.11 rows=1045726 width=18) (actual time=0.010..639.642 rows=1044189 loops=1)
--                                                  Heap Fetches: 0
--                    ->  Hash  (cost=1013.11..1013.11 rows=33121 width=16) (actual time=17.933..17.934 rows=33121 loops=1)
--                          Buckets: 65536  Batches: 1  Memory Usage: 2065kB
--                          ->  Index Only Scan using idx_flights_aircraft_code_flight_id_scheduled_departure on flights f  (cost=0.29..1013.11 rows=33121 width=16) (actual time=0.891..9.074 rows=33121 loops=1)
--                                Heap Fetches: 0
--              ->  Hash  (cost=43.24..43.24 rows=9 width=12) (actual time=28.942..28.944 rows=9 loops=1)
--                    Buckets: 1024  Batches: 1  Memory Usage: 9kB
--                    ->  Subquery Scan on ts  (cost=0.28..43.24 rows=9 width=12) (actual time=27.921..28.903 rows=9 loops=1)
--                          ->  GroupAggregate  (cost=0.28..43.15 rows=9 width=12) (actual time=27.917..28.891 rows=9 loops=1)
--                                Group Key: seats.aircraft_code
--                                ->  Index Only Scan using idx_seats_aircraft_code on seats  (cost=0.28..36.36 rows=1339 width=4) (actual time=4.034..4.767 rows=1339 loops=1)
--                                      Heap Fetches: 0
--Planning Time: 1.059 ms
--JIT:
--  Functions: 32
--  Options: Inlining false, Optimization false, Expressions true, Deforming true
--  Timing: Generation 2.361 ms, Inlining 0.000 ms, Optimization 0.890 ms, Emission 23.103 ms, Total 26.353 ms
--Execution Time: 5785.177 ms




--Задача 3. Узнать что такое GroupAggregate и придумать пример имитации его

--GroupAggregate — это метод выполнения агрегатных функций в PostgreSQL, который используется при group by. Он проходит по входным данным построчно, выполняя агрегатные вычисления для каждой группы.

--Когда используется GroupAggregate:

--   - Когда PostgreSQL видит GROUP BY, но не может использовать HashAggregate.

--   - Когда данные уже отсортированы по группировочному столбцу.

--   - Когда агрегатные функции требуют построчной обработки (например, ARRAY_AGG).

--Разница с HashAggregate:

--GroupAggregate работает последовательно, проходя по данным.

--HashAggregate создаёт хеш-таблицу, чтобы быстрее группировать данные.

set enable_hashagg = off;
explain analyze
select departure_airport, count(flight_id) as flight_count
from bookings.flights
group by departure_airport
order by flight_count desc;

--Sort  (cost=3462.78..3463.04 rows=104 width=12) (actual time=31.966..31.975 rows=104 loops=1)
--  Sort Key: (count(flight_id)) DESC
--  Sort Method: quicksort  Memory: 29kB
--  ->  GroupAggregate  (cost=3209.85..3459.29 rows=104 width=12) (actual time=22.335..31.927 rows=104 loops=1)
--        Group Key: departure_airport
--        ->  Sort  (cost=3209.85..3292.65 rows=33121 width=8) (actual time=22.289..24.479 rows=33121 loops=1)
--              Sort Key: departure_airport
--              Sort Method: quicksort  Memory: 2572kB
--              ->  Seq Scan on flights  (cost=0.00..723.21 rows=33121 width=8) (actual time=0.010..5.097 rows=33121 loops=1)
--Planning Time: 0.060 ms
--Execution Time: 32.045 ms


