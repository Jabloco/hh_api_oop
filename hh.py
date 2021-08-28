import requests
import json
import time
import psycopg2
import db_setting # файл с настройками для БД
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import re

class Headhunter:
    def __init__(self, text = 'python junior', area = 113):
        self.text = text
        self.area = area
        
    def GetVacancysList(self):
       
        """
        Метод для полуяения списка ссылок на вакансии. \n
        Без аргументов.\n
        Внутри себя вызывает приватный метод __GetPage\n
        """
       
        self.url_list = []

        for page in range(20):
            js_str = self.__GetPage(page) #получаем ответ в виде json - файла
            js = json.loads(js_str) # Преобразуем текст ответа запроса в словарь Python
            # добавляем url в список ссылок
            self.url_list.extend([js['items'][i]['url'] for i in range(len(js['items']))])
            if (js['pages'] - page) <= 1:
                break
            time.sleep(0.5)
        return self.url_list

    def __GetPage(self, page = 0):
        """
        Функция для получения страницы со списком вакансий.\n
        Аргументы:\n
            page - Индекс страницы (число), начинается с 0. Значение по умолчанию 0, т.е. первая страница\n
        """
        # Словарь для параметров GET-запроса
        params = {
            'text': self.text,
            'page': page, # Индекс страницы поиска на HH
            'per_page': 100, # Кол-во вакансий на 1 странице
            'area': self.area 
        }
        
        req = requests.get('https://api.hh.ru/vacancies', params) # Посылаем запрос к API
        data = req.content.decode() # Декодируем его ответ, чтобы Кириллица отображалась корректно
        req.close()
        return data
            
        
    def GetVacancyDetail(self, url):
        """
        Метод для получения детальной информации о вакансии\n
        Аргументы:\n
            url - ссылка (строка) на вакансию в формате api (например: https://api.hh.ru/vacancies/44528998?host=hh.ru)\n
        """
        self.url = url
        req = requests.get(self.url)
        # with open('vacancy_detail.json', 'w', encoding='utf8') as f:
        #     f.write(req.content.decode())
        self.detail = json.loads(req.content.decode())
        req.close()
        time.sleep(0.3)
        return self.detail


class DatabaseWorker():
    def __init__(self):
        self.login = db_setting.LOGIN
        self.password = db_setting.PASSWORD
        self.host = db_setting.HOST
        self.port = db_setting.PORT
        self.database = db_setting.DB_NAME

    def SqlRequest(self, request):
        try:
            # кортеж параметров для подключения к БД
            connection = psycopg2.connect(
                user = self.login,
                password = self.password,
                host = self.host,
                port = self.port,
                database = self.database
            )
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = connection.cursor()
            cursor.execute(request)
            if re.match(r'SELECT', request, re.IGNORECASE) is not None: # регулярка для поиска SELECT в начале строки
                return cursor.fetchall()
        except (Exception, psycopg2.Error) as error:
            print("Что-то пошло не так", error)
        finally:
            if connection:
                cursor.close()
                connection.close

db = DatabaseWorker()
req = """SELect name FROM keyskill where name = 'Python'"""
print(db.SqlRequest(req))
#     def InsertToBase(self, table_name, columns, values):
#         """
#         Метод для записи в базу данных\n
#         Аргументы:\n
#             table_name - имя таблицы (строка)\n
#             columns - колонки (строка)\n
#             values - значения (кортеж)\n
#         """
#         try:
#             # кортеж параметров для подключения к БД
#             connection = psycopg2.connect(
#                 user = self.login,
#                 password = self.password,
#                 host = self.host,
#                 port = self.port,
#                 database = self.database
#             )
#             connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
#             cursor = connection.cursor()
#             sql_insert = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(['%s'] * len(values))});"
#             cursor.execute(sql_insert, values)
#         except (Exception, psycopg2.Error) as error:
#             print("Что-то пошло не так", error)
#         finally:
#             if connection:
#                 cursor.close()
#                 connection.close

#     def SelectFromBase(self, columns, table_name, filter_colomn='', filter_value=''):
#         """
#         Метод получения данных из базы.\n
#         Аргументы:\n
#             columns - колонки, которые считываем (строка)\n
#             table_name - таблица в которой ищем (строка)\n
#             Фильтрация результатов.
#             filter_colomn - колонка, по которой ищем (строка)\n
#             filter_value - значение по которому ищем (строка)\n
#             Передается ТОЛЬКО по подному аргументу фильтрации.
#         """
#         try:
#             # кортеж параметров для подключения к БД
#             connection = psycopg2.connect( 
#                 user = self.login,
#                 password = self.password,
#                 host = self.host,
#                 port = self.port,
#                 database = self.database
#             )
#             cursor = connection.cursor()
#             # проверка на наличие фильтров для запроса к БД
#             if filter_colomn == '': # если не указан фильтрующий столбец
#                 sql_select = """SELECT {col} FROM {t_name};"""
#             elif filter_value == '': # если не указано фильтрующее значение в столбце
#                 sql_select = """SELECT {col} FROM {t_name} WHERE {f_col} IS NOT NULL;"""
#             else:
#                 sql_select = """SELECT {col} FROM {t_name} WHERE {f_col} = $${f_val}$$;""" # «заключение строк в доллары» для обхода ' в фильтрующих значениях (напр. Л'Этуаль)
#                 # https://postgrespro.ru/docs/postgresql/9.6/sql-syntax-lexical пункт 4.1.2.4
#             cursor.execute(sql_select.format(col = columns, t_name = table_name, f_col = filter_colomn, f_val = filter_value))
#             return cursor.fetchall()
#         except (Exception, psycopg2.Error) as error:
#             print("Что-то пошло не так", error)
#         finally:
#             if connection:
#                 cursor.close()
#                 connection.close

#     def JoinTables(self):
#         pass

# vacancys = Headhunter()
# x = DatabaseWorker()
# for url in vacancys.GetVacancysList():
#     # что бы не делать запрос к API каждый раз
#     vacancy_detail_dict = vacancys.GetVacancyDetail(url)
    
#     # проверяем наличие hh_id в локальной БД что бы не дергать базу одними и теми же вакансиями
#     # и начинаем проверку других таблиц только если hh_id нет в базе
#     if vacancy_detail_dict['id'] not in x.SelectFromBase('hh_id', 'vacancy', 'hh_id', vacancy_detail_dict['id']):
#         # запись ключевых навыков в БД
#         skill_list = [skill['name'] for skill in vacancy_detail_dict['key_skills']]
#         for skill in skill_list:
#             if skill not in [skill[0] for skill in x.SelectFromBase('name', 'keyskill', 'name', skill)]:
#                 x.InsertToBase('keyskill', 'name', (skill,))
        
#         # запись городов в БД
#         city_name = vacancy_detail_dict['area']['name']
#         if city_name not in [city[0] for city in x.SelectFromBase('name', 'city', 'name', city_name)]:
#             x.InsertToBase('city', 'name', (city_name,) )

#         # запись работодателей в БД
#         employer_tuple = (vacancy_detail_dict['employer']['name'], vacancy_detail_dict['employer']['url'])
#         if employer_tuple[0] not in [employer[0] for employer in x.SelectFromBase('name', 'employer', 'name', vacancy_detail_dict['employer']['name'])]:
#             x.InsertToBase('employer', 'name, url', employer_tuple)

#         # обработка пустых значений вознаграждения
#         if vacancy_detail_dict['salary'] is None:
#             salary_from = 0
#             salary_to = 0
#             salary_currency = 'n/n'
#         else:
#             salary_from = vacancy_detail_dict['salary']['from']

#             if vacancy_detail_dict['salary']['to'] is None:
#                 salary_to = 0
#             else:
#                 salary_to = vacancy_detail_dict['salary']['to']
#             salary_currency = vacancy_detail_dict['salary']['currency']
        
#         # кортеж с данными о вакансии
#         vacancy_detail_tuple = (
#             vacancy_detail_dict['id'], #строка
#             vacancy_detail_dict['name'],
#             salary_from,
#             salary_to,
#             salary_currency,
#             vacancy_detail_dict['description'],
#             vacancy_detail_dict['created_at'][:10],
#             [id[0] for id in x.SelectFromBase('id', 'city', 'name', vacancy_detail_dict['area']['name'])][0],
#             [id[0] for id in x.SelectFromBase('id', 'employer', 'name', vacancy_detail_dict['employer']['name'])][0]
#         )

#         # запись вакансии в БД
#         if int(vacancy_detail_tuple[0]) not in [vacancy_id[0] for vacancy_id in x.SelectFromBase('hh_id', 'vacancy', 'hh_id', vacancy_detail_tuple[0])]:
#             x.InsertToBase(
#                 'vacancy',
#                 'hh_id, name, salary_from, salary_to, salary_currency, description, date_create, city_id, employer_id',
#                 vacancy_detail_tuple
#                 )
        
#         # запись связующей таблицы vacancy_skill
#         vacancy_id = [id[0] for id in x.SelectFromBase('id', 'vacancy', 'hh_id', vacancy_detail_dict['id'])]
#         skill_id_list = [[id[0] for id in x.SelectFromBase('id', 'keyskill', 'name', skill)][0] for skill in skill_list]
#         vacancy_id_skill_id_pair = [(vac_id, s_id) for vac_id in vacancy_id for s_id in skill_id_list]
#         for pair in vacancy_id_skill_id_pair:
#             if pair not in [pair for pair in x.SelectFromBase('vacancy_id, keyskill_id', 'vacancy_skill', 'vacancy_id', vacancy_id[0])]:
#                 x.InsertToBase('vacancy_skill', 'vacancy_id, keyskill_id', pair)

# select vacancy.name, keyskill.name, employer.name
# from 
#   vacancy 
# left join 
#   employer 
#       on vacancy.employer_id = employer.id
# left join
#   vacancy_skill
#       on vacancy.id = vacancy_skill.vacancy_id
# left join
#   keyskill
#       on vacancy_skill.keyskill_id = keyskill.id
# where
#   keyskill.name='Python';

# посчитать в скольких выкансиях встечается навык, например, Python
# select keyskill.name, count(*) 
# from keyskill 
# left join vacancy_skill on vacancy_skill.keyskill_id = keyskill.id
# where vacancy_skill.keyskill_id = (select id from keyskill where name='Python')
# group by keyskill.name;

# вывод ключевых навыков со счетчиком для каждого

# select keyskill.name, count(*)
# from keyskill
# left join vacancy_skill on vacancy_skill.keyskill_id = keyskill.id
# group by keyskill.name # группировка поимени навыка
# order by count desc # сортировка по возрастанию счетчика
# limit 25; # лимит в 25 строк
