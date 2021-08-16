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
        Метод для полуяения списка ссылок на вакансии. \n
        Аргументы:\n
            text - поисковый запрос\n
            area - регион поиска. 113 - Россия\n
        """
        self.text = text
        self.area = area

        def getPage(page = 0):
            """
            Функция для получения страницы со списком вакансий. \n
            Аргументы:\n
                page - Индекс страницы, начинается с 0. Значение по умолчанию 0, т.е. первая страница\n
            """
            # Словарь для параметров GET-запроса
            params = {
                'text': self.text,
                'page': page, # Индекс страницы поиска на HH
                'per_page': 1, # Кол-во вакансий на 1 странице
                'area': self.area 
            }
        
            req = requests.get('https://api.hh.ru/vacancies', params) # Посылаем запрос к API
            data = req.content.decode() # Декодируем его ответ, чтобы Кириллица отображалась корректно
            req.close()
            return data
            
        self.url_list = []
        for page in range(1):
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
        Метод для получения детальной информации о вакансии\n
        Аргументы:\n
            url - ссылка на вакансию в формате api (например: https://api.hh.ru/vacancies/44528998?host=hh.ru)\n
        """
        self.url = url
        req = requests.get(self.url)
        # with open('vacancy_detail.json', 'w', encoding='utf8') as f:
        #     f.write(req.content.decode())
        self.detail = json.loads(req.content.decode())
        req.close()
        time.sleep(0.1)
        return self.detail


    def InsertToBase(self, table_name, columns, values):
        """
        Метод для записи в базу данных\n
        Аргументы:\n
            table_name - имя таблицы\n
            columns - колонки\n
            values - значения\n
        """
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
            sql_insert = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(['%s'] * len(values))});"
            print(sql_insert)
            cursor.execute(sql_insert, values)
        except (Exception, psycopg2.Error) as error:
            print("Что-то пошло не так", error)
        finally:
            if connection:
                cursor.close()
                connection.close

    def SelectFromBase(self, columns, table_name):
        """
        Метод получения данных из базы.\n
        Аргументы:\n
            columns - колонки, которые считываем\n
            table_name - таблица в которой ищем\n
        """
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

        def JoinTables(self):
            pass

x = Headhunter()

print('fg' in x.SelectFromBase('name', 'country'))
# x.GetVacancyDetail(x.GetVacancyList()[0])
