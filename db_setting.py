LOGIN = 'postgres'
PASSWORD = 'password'
HOST = 'localhost'
PORT = '5432'
DB_NAME = 'vacancys'

TABLES_TUPLE = (
    # страна
    """CREATE TABLE country( 
        id serial primary key,
        name varchar(255)
        );""",
    # город
    """CREATE TABLE city(
        id serial primary key,
        name varchar(255),
        country_id integer references country(id)
        );""",
    # работодатель
    """CREATE TABLE employer(
        id serial primary key,
        name varchar(255),
        url text
        );""",
    # ключевые навыки
    """CREATE TABLE keyskill(
        id serial primary key,
        name varchar(255)
        );""",
    # вакансия
    """CREATE TABLE vacancy(
        id serial primary key,
        hh_id integer,
        name varchar(255),
        salary money,
        description text,
        date_create date,
        city_id integer references city(id),
        employer_id  integer references employer(id)
        );""",
    # связующая таблица вакансия - скилы
    """CREATE TABLE vacancy_skill(
        vacancy_id integer references vacancy(id),
        keyskill_id integer references keyskill(id),
        primary key(vacancy_id, keyskill_id)
        );"""
)