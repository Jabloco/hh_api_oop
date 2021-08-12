import psycopg2
import db_setting

class PostgresWorker():
    def __init__(self):
        pass
    
    def create_database(self):
        pass

    def create_table(self):
        pass




try:
    connection = psycopg2.connect(
        user = db_setting.LOGIN,
        password = db_setting.PASSWORD,
        host = db_setting.HOST,
        port = db_setting.PORT,
        database = db_setting.DB_NAME
    )
    cursor = connection.cursor()
    print(connection.get_dsn_parameters(), '\n')
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print(record)
except (Exception, psycopg2.Error) as error:
    print("Что-то пошло не так", error)
finally:
    if connection:
        cursor.close()
        connection.close