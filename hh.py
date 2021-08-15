import requests
import json
import time
import psycopg2
import db_setting
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

class Headhunter():
    def __init__(self):
        self.login = db_setting.LOGIN
        self.password = db_setting.PASSWORD
        self.host = db_setting.HOST
        self.port = db_setting.PORT
        self.database = db_setting.DB_NAME

    def GetVacancyList(self, text="python junior", area=113):
        """
        Метод для полуяения списка ссылок на вакансии.
        Аргументы:
            text - поисковый запрос
            area - регион поиска. 113 - Россия
        """
        self.text = text
        self.area = area

        def getPage(page = 0):
            """
            Функция для получения страницы со списком вакансий.
            Аргументы:
                page - Индекс страницы, начинается с 0. Значение по умолчанию 0, т.е. первая страница
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
            
        self.url_list = []
        for page in range(20):
            js_str = getPage(page) #получаем ответ в виде json - файла
            js = json.loads(js_str) # Преобразуем текст ответа запроса в словарь Python
            # добавляем url в список ссылок
            self.url_list.extend([js['items'][i]['url'] for i in range(len(js['items']))])
            if (js['pages'] - page) <= 1:
                break
            time.sleep(0.1)
        return self.url_list

    def GetVacancyDetail(self, url):
        """
        Метод для получения детальной информации о вакансии
        Аргументы:
            url - ссылка на вакансию в формате api (например: https://api.hh.ru/vacancies/44528998?host=hh.ru)
        """
        self.url = url
        req = requests.get(self.url)
        # with open('vacancy_detail.json', 'w', encoding='utf8') as f:
        #     f.write(req.content.decode())
        self.detail = json.loads(req.content.decode())
        req.close()
        time.sleep(0.1)
        return self.detail


    def WriteToBase(self, table_name, columns, values):
        try:
            connection = psycopg2.connect(
                user = self.login,
                password = self.password,
                host = self.host,
                port = self.port,
                database = self.database
            )
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = connection.cursor()
            # вокруг values какой-то костыль, мне не нравиться =(
            # sql_insert = f"INSERT INTO {table_name} ({columns}) VALUES ('{values}');"  # работает для вставки в один столбец
            sql_insert = f"INSERT INTO {table_name} ({columns}) VALUES {values};"  # работает для несколькиих столбцов, но не работает для одного
            # вокруг values какой-то костыль, мне не нравиться =(
            cursor.execute(sql_insert)
        except (Exception, psycopg2.Error) as error:
            print("Что-то пошло не так", error)
        finally:
            if connection:
                cursor.close()
                connection.close

    def ReadFromBase(self, columns, table_name):
        try:
            connection = psycopg2.connect(
                user = self.login,
                password = self.password,
                host = self.host,
                port = self.port,
                database = self.database
            )
            cursor = connection.cursor()
            sql_select = f"SELECT {columns} FROM {table_name};"
            cursor.execute(sql_select)
            return cursor.fetchall()
        except (Exception, psycopg2.Error) as error:
            print("Что-то пошло не так", error)
        finally:
            if connection:
                cursor.close()
                connection.close

x = Headhunter()


# table_name = 'vacancy'
# columns = 'hh_id, name , salary, description'
# values = '798', 'Python', '100000', 'dkdfvfdk8954894xfkjdnnj'

table_name = 'country'
columns = 'name'
values = '798ffj'
print(values)
x.WriteToBase(table_name,columns,values)
print(x.ReadFromBase(columns, table_name))
print()
