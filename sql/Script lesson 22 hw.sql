--Задание для самостоятельной работы:
--Описание задачи
--Необходимо создать базу данных для интернет-магазина "TechGadgets" с разграничением прав доступа для разных групп сотрудников.

--Требования к реализации
--Структура базы данных:
-- Создать БД tech_gadgets
-- Создать 3 схемы: production, analytics, sandbox

--Таблицы:
-- В схеме production: products, customers, orders, order_items
-- В схеме analytics: sales_stats, customer_segments
-- В схеме sandbox: оставить пустой (для экспериментов аналитиков)

create database TechGadgets;

create schema production;
create schema analytics;
create schema sandbox;

-- Таблицы в схеме production
CREATE TABLE production.products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(50),
    stock_quantity INT NOT NULL
);

CREATE TABLE production.customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    registration_date DATE NOT NULL
);

CREATE TABLE production.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES production.customers(customer_id),
    order_date TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL
);

CREATE TABLE production.order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES production.orders(order_id),
    product_id INT REFERENCES production.products(product_id),
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL
);

-- Таблицы в схеме analytics
CREATE TABLE analytics.sales_stats (
    stat_id SERIAL PRIMARY KEY,
    period DATE NOT NULL,
    total_sales DECIMAL(12,2) NOT NULL,
    top_product_id INT REFERENCES production.products(product_id)
);

CREATE TABLE analytics.customer_segments (
    segment_id SERIAL PRIMARY KEY,
    segment_name VARCHAR(50) NOT NULL,
    criteria TEXT NOT NULL,
    customer_count INT NOT NULL
);

--Роли и права:
-- Дата-инженеры: полный доступ ко всем схемам и таблицам
-- Аналитики: чтение из всех схем + полный доступ к схеме sandbox
-- Менеджеры: чтение только из схемы analytics

create role data_engineer login password 'secret123';

grant connect on database TechGadgets to data_engineer;
grant all privileges on schema production, analytics, sandbox to data_engineer;
grant all privileges on all tables in schema production to data_engineer;
grant all privileges on all tables in schema analytics to data_engineer;
grant all privileges on all tables in schema sandbox to data_engineer;

create role analyst login password 'secret456';

grant connect on database TechGadgets to analyst;
grant usage on schema production, analytics to analyst;
grant select on all tables in schema production to analyst;
grant select on all tables in schema analytics to analyst;
grant all privileges on schema sandbox to analyst;
grant all privileges on all tables in schema sandbox to analyst;


create role manager login password 'secret789';

grant connect on database TechGadgets to manager;
grant usage on schema analytics to manager;
grant select on all tables in schema analytics to manager;


--Домашнее задание 22 :
-- Создайте представление (VIEW) в схеме analytics с продажами по категориям
-- В запросе посчитать сумму продаж по категориям

create view analytics.sales_by_category as
SELECT p.category, SUM(oi.quantity) as total_quantity, SUM(oi.price * oi.quantity) as total_sales
FROM production.order_items oi
JOIN production.products p ON oi.product_id = p.product_id
GROUP BY p.category;


-- Дайте доступ менеджерам только к этому представлению, а не ко всем таблицам

revoke select on all tables in schema analytics from manager;
grant select on analytics.sales_by_category to manager;


-- Создайте роль senior_analysts с правами аналитиков + возможностью создавать временные таблицы

create role senior_analyst login password 'secret098';
grant analyst to senior_analyst;
grant temp on database TechGadgets to senior_analyst;


