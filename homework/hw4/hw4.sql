SET schema 'hw4';
drop table if exists movies cascade;


/* Создайте таблицу movies с полями
 * movies_type, director, year_of_issue, length_in_minutes, rate. */
CREATE TABLE movies(
	movies_type character varying not null,
	director character varying not null,
	year_of_issue int not null,
	length_in_minutes int not null,
	rate int not null
);


/* Сделайте таблицы для горизонтального партицирования по году выпуска
 * (до 1990, 1990 -2000, 2000- 2010, 2010-2020, после 2020). */
create table year_of_issue_1990(
	check(year_of_issue <= 1990)
) inherits(movies);

create table year_of_issue_1990_2000(
	check(year_of_issue > 1990 and year_of_issue <= 2000)
) inherits(movies);

create table year_of_issue_2000_2010(
	check(year_of_issue > 2000 and year_of_issue <= 2010)
) inherits(movies);

create table year_of_issue_2010_2020(
	check(year_of_issue > 2010 and year_of_issue <= 2020)
) inherits(movies);

create table year_of_issue_2020(
	check(year_of_issue > 2020)
) inherits(movies);


/* Сделайте таблицы для горизонтального партицирования по длине фильма
 * (до 40 минута, от 40 до 90 минут, от 90 до 130 минут, более 130 минут). */
create table length_in_minutes_40(
	check(length_in_minutes <= 40)
) inherits(movies);

create table length_in_minutes_40_90(
	check(length_in_minutes > 40 and length_in_minutes <= 90)
) inherits(movies);

create table length_in_minutes_90_130(
	check(length_in_minutes > 90 and length_in_minutes <=130)
) inherits(movies);

create table length_in_minutes_130(
	check(length_in_minutes > 130)
) inherits(movies);


/* Сделайте таблицы для горизонтального партицирования
 * по рейтингу фильма (ниже 5, от 5 до 8, от 8до 10). */
create table rate_5(
	check(rate < 5)
) inherits(movies);

create table rate_5_8(
	check(rate >= 5 and rate < 8)
) inherits(movies);

create table rate_8_10(
	check(rate >= 8 and rate <= 10)
) inherits(movies);


/* Создайте правила добавления данных для каждой таблицы. */
/* year_of_issue */
create rule insert_year_of_issue_1990
as on insert to movies
where(year_of_issue <= 1990) do instead 
insert into year_of_issue_1990 values(new.*);

create rule insert_year_of_issue_1990_2000
as on insert to movies
where(year_of_issue > 1990 and year_of_issue <= 2000) do instead 
insert into year_of_issue_1990_2000 values(new.*);

create rule insert_year_of_issue_2000_2010
as on insert to movies
where(year_of_issue > 2000 and year_of_issue <= 2010) do instead 
insert into year_of_issue_2000_2010 values(new.*);

create rule insert_year_of_issue_2010_2020
as on insert to movies
where(year_of_issue > 2010 and year_of_issue <= 2020) do instead 
insert into year_of_issue_2010_2020 values(new.*);

create rule insert_year_of_issue_2020
as on insert to movies
where(year_of_issue > 2020) do instead 
insert into year_of_issue_2020 values(new.*);

/* length_in_minutes */
create rule insert_length_in_minutes_40
as on insert to movies
where(length_in_minutes <= 40) do instead 
insert into length_in_minutes_40 values(new.*);

create rule insert_length_in_minutes_40_90
as on insert to movies
where(length_in_minutes > 40 and length_in_minutes <= 90) do instead 
insert into length_in_minutes_40_90 values(new.*);

create rule insert_length_in_minutes_90_130
as on insert to movies
where(length_in_minutes > 90 and length_in_minutes <=130) do instead 
insert into length_in_minutes_90_130 values(new.*);

create rule insert_length_in_minutes_130
as on insert to movies
where(length_in_minutes > 130) do instead 
insert into length_in_minutes_130 values(new.*);

/* rate */
create rule insert_rate_5
as on insert to movies
where(rate < 5) do instead 
insert into rate_5 values(new.*);

create rule insert_rate_5_8
as on insert to movies
where(rate >= 5 and rate < 8) do instead 
insert into rate_5_8 values(new.*);

create rule insert_rate_8_10
as on insert to movies
where(rate >= 8 and rate <= 10) do instead 
insert into rate_8_10 values(new.*);


/* Добавьте фильмы так, чтобы в каждой таблице было не менее 3 фильмов. */
insert into movies
	(movies_type, director, year_of_issue, length_in_minutes, rate)
	values
	('type_1', 'director_1', 1970, 42, 3),
	('type_2', 'director_2', 1989, 95, 6),
	('type_3', 'director_3', 1960, 136, 10),
	('type_4', 'director_4', 1993, 47, 4),
	('type_5', 'director_1', 1994, 58, 3),
	('type_6', 'director_2', 1999, 30, 7),
	('type_7', 'director_3', 2010, 40, 5),
	('type_8', 'director_4', 2005, 36, 8),
	('type_9', 'director_1', 2007, 100, 9),
	('type_10', 'director_1', 2017, 105, 7),
	('type_11', 'director_1', 2015, 110, 9),
	('type_12', 'director_3', 2020, 140, 5),
	('type_13', 'director_3', 2021, 146, 6),
	('type_14', 'director_2', 2022, 160, 10),
	('type_15', 'director_4', 2023, 180, 4);


/* Добавьте пару фильмов с рейтингом выше 10. */
insert into movies
	(movies_type, director, year_of_issue, length_in_minutes, rate)
	values
	('type_16', 'director_5', 1970, 42, 13),
	('type_17', 'director_2', 1989, 95, 16);


/* Сделайте выбор из всех таблиц, в том числе из основной. */
select * from year_of_issue_1990;
select * from year_of_issue_1990_2000;
select * from year_of_issue_2000_2010;
select * from year_of_issue_2010_2020;
select * from year_of_issue_2020;
select * from length_in_minutes_40;
select * from length_in_minutes_40_90;
select * from length_in_minutes_90_130;
select * from length_in_minutes_130;
select * from rate_5;
select * from rate_5_8;
select * from rate_8_10;

select * from movies;

/* Сделайте выбор только из основной таблицы. */
select * from only movies;



