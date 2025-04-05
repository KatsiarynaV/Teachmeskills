-- 1. Создание таблицы promocodes
-- Что нужно сделать:
-- Создать таблицу для хранения промокодов, которые могут применять пользователи к своим заказам.

-- Требования:
-- promo_id – уникальный ID промокода (первичный ключ, автоинкремент).
-- code – текст промокода (уникальный, не может повторяться).
-- discount_percent – процент скидки (от 1 до 100).
-- valid_from и valid_to – срок действия.
-- max_uses – максимальное количество применений (NULL = без ограничений).
-- used_count – сколько раз уже использован (по умолчанию 0).
-- is_active – активен ли промокод (1 или 0, по умолчанию 1).
-- created_by – кто создал промокод (внешний ключ на users.id).

create table promocodes (
	promo_id integer primary key autoincrement,
	code text not null unique,
	discount_percent integer check(discount_percent between 1 and 100),
	valid_from datetime default current_timestamp,
	valid_to datetime not null,
	max_uses integer default NULL,
	used_count integer default 0,
	is_active integer default 1 check(is_active in(0, 1)),
	created_by integer not null,
	FOREIGN KEY (created_by) REFERENCES users(id)
);


-- 2. Заполнение таблицы тестовыми данными
-- Добавить 10 тестовых промокодов с разными параметрами:
-- Скидки от 5% до 50%.
-- Разные даты действия (некоторые уже истекли).
-- Ограниченное и неограниченное количество использований.

insert into promocodes (code, discount_percent, valid_from, valid_to, max_uses, used_count, is_active, created_by)
values
	('SUMMER10', 10, '2024-06-01', '2025-08-31', 100, 10, 1, 3),
    ('WELCOME20', 20, '2025-01-01', '2025-12-31', NULL, 2, 1, 4),
    ('BLACKFRIDAY30', 30, '2024-11-24', '2025-01-27', 500, 15, 0, 5),
    ('NEWYEAR15', 15, '2024-12-20', '2025-01-10', 200, 4, 0, 6),
    ('FLASH25', 25, '2024-10-01', '2025-10-07', 50, 5, 1, 7),
    ('LOYALTY5', 5, '2025-01-01', '2025-01-01', NULL, 2, 0, 8),
    ('MEGA50', 50, '2024-09-01', '2025-09-30', 10, 9, 1, 9),
    ('AUTUMN20', 20, '2024-09-01', '2025-11-30', 300, 12, 1, 10),
    ('SPRING10', 10, '2025-03-01', '2025-05-31', 150, 6, 1, 11),
    ('VIP40', 40, '2024-07-01', '2025-07-31', 20, 5, 1, 12);


select * from promocodes;

-- 3. Анализ по группам скидок
-- Сгруппировать промокоды по диапазонам скидок и вывести:
-- Количество промокодов в группе.
-- Минимальную и максимальную скидку.
-- Сколько из них имеют ограничение по использованию (max_uses IS NOT NULL).

select
	CASE 
		when discount_percent between 1 and 10 then 'Discount 1-10%'
		when discount_percent between 11 and 30 then 'Discount 11-30%'
		when discount_percent between 31 and 40 then 'Discount 31-40%'
		when discount_percent >= 50 then 'Discount 50+%'
		else 'Неизвсетно'
	END as discount_percent_group,
	count(*) as promocodes_count,
	max(discount_percent) as max_discount_percent,
	min(discount_percent) as min_discount_percent,
	count(CASE when max_uses IS NOT NULL then 1 end) as restricted_usage_count
from promocodes
group by discount_percent_group;
	

-- 4. Анализ по времени действия
-- Что нужно сделать:
-- Разделить промокоды на:
-- Активные (текущая дата между valid_from и valid_to).
-- Истекшие (valid_to < текущая дата).
-- Еще не начавшиеся (valid_from > текущая дата).
-- Для каждой группы вывести:
-- Количество промокодов.
-- Средний процент скидки.
-- Сколько из них имеют лимит использований.


select
	CASE
		when date('now') between valid_from and valid_to then 'Active promocodes'
		when date('now') > valid_to then 'Expired promocodes'
		when date('now') < valid_from then 'Not active yet'
		else 'Неизвестно'
	END as promocodes_status,
	count(*) as promocodes_count,
	round(avg(discount_percent), 2) as avg_discount_percent,
	count(CASE when max_uses IS NOT NULL then 1 end) as restricted_usage_count
from promocodes
group by promocodes_status
	







