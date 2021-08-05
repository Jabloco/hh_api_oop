import requests
import json
import time

class Headhunter():
    def __init__(self):
        pass

    def GetVacancyList(self, text="python junior", area=113):
        def getPage(page = 0):
            """
            Создаем функцию для получения страницы со списком вакансий.
            Аргументы:
                page - Индекс страницы, начинается с 0. Значение по умолчанию 0, т.е. первая страница
            """
            # Словарь для параметров GET-запроса
            params = {
                'text': text, # Текст фильтра
                'page': page, # Индекс страницы поиска на HH
                'per_page': 100, # Кол-во вакансий на 1 странице
                'area': area # регион поиска. 113 - Россия
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
        self.url = url
        req = requests.get(self.url)
        # with open('vacancy_detail.json', 'w', encoding='utf8') as f:
        #     f.write(req.content.decode())
        self.detail = json.loads(req.content.decode())
        req.close()
        time.sleep(0.1)
        return self.detail


    def WriteToBase(self):
        pass

    def ReadFromBase(self):
        pass

x = Headhunter()
# for i in x.GetVacancyList():
#     print(i)

# with open('vacancy_detail.json', 'w') as f:
#     f.write(str(x.GetVacancyDetail('https://api.hh.ru/vacancies/44528998?host=hh.ru')))

print(x.GetVacancyDetail('https://api.hh.ru/vacancies/44528998?host=hh.ru')['description'])