create table books (
	book_id integer primary key,
	book_title text,
	publish_year integer,
	author_id integer,
	available_count integer,
	book_status text,
	FOREIGN KEY (author_id) REFERENCES authors(author_id)
);

create table authors (
	author_id integer primary key,
	author_name text,
	author_birth_year integer,
	author_country text,
	author_biography text
);

create table readers (
	reader_id integer primary key,
	reader_name text,
	reader_library_card_number text,
	registration_date date,
	reader_phone_number text,
	email text
);

insert into authors (author_name, author_birth_year, author_country, author_biography)
values 
	('Лев Толстой', 1828, 'Россия', 'Один из наиболее известных русских писателей и мыслителей, один из величайших в мире писателей‑романистов'),
	('Федор Достоевский', 1821, 'Россия', 'Русский писатель, мыслитель, философ и публицист'),
	('Михаил Булгаков', 1891, 'Россия', 'Русский писатель советского периода, врач, драматург, театральный режиссёр и актёр'),
	('Стивен Кинг', 1947, 'США', 'Американский писатель, работающий в разнообразных жанрах, включая ужасы, триллер, фантастику, фэнтези, мистику, драму, детектив'),
	('Жан-Кристоф Гранже', 1961, 'Франция', 'Журналист, международный репортер, писатель и сценарист ');

insert into books (book_title, publish_year, author_id, available_count, book_status)
values 
	('Война и мир', 1867, 1, 4, 'доступна'),
	('Анна Каренина', 1878, 1, 3, 'на руках'),
	('Преступление и наказание', 1866, 2, 5, 'не доступна'),
	('Братья Карамазовы', 1880, 2, 2, 'доступна'),
	('Мастер и Маргарита', 1967, 3, 6, 'доступна'),
	('Собачье сердце', 1968, 3, 2, 'на руках'),
	('Белая гвардия', 1925, 3, 3, 'доступна'),
	('Оно', 1986, 4, 1, 'на руках'),
	('Сияние', 1977, 4, 3, 'доступна'),
	('Под куполом', 2009, 4, 1, 'не доступна'),
	('Зеленая миля', 1996, 4, 3, 'доступна'),
	('Пурпурные реик', 1997, 5, 4, 'доступна'),
	('Черная линия', 2004, 5, 3, 'на руках'),
	('Империя волков', 2002, 5, 1, 'доступна');

insert into readers(reader_name, reader_library_card_number, registration_date, reader_phone_number, email)
values
	('Иван Иванов', 'A156326', '2020-01-15', '80296321265', 'ivan.ivanov@example.com'),
	('Анна Петрова', 'A234567', '2021-03-12', '80295632565', 'anna.petrova@example.com'),
	('Сергей Смирнов', 'B345678', '2022-07-20', '80298965231', 'sergey.smirnov@example.com'),
	('Мария Васильева', 'R456789', '2019-11-01', '80297584213', 'maria.vasilyeva@example.com'),
	('Дмитрий Фёдоров', 'T567890', '2023-02-05', '80297123658', 'dmitry.fedorov@example.com');

Написать запросы для выборки:
- Всех книг конкретного автора
- Всех доступных книг
- Читателей, зарегистрированных в определенный период

select * from books 
where author_id = 4

select * from books 
where book_status = 'доступна'

select * from readers 
where registration_date between '2021-01-01' and '2022-12-31'


