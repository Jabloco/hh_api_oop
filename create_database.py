import psycopg2
import db_setting
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class PostgresWorker():
    def __init__(self):
        self.login = db_setting.LOGIN
        self.password = db_setting.PASSWORD
        self.host = db_setting.HOST
        self.port = db_setting.PORT
        self.tables = db_setting.TABLES_TUPLE
    
    def create_database(self):
        try:
            connection = psycopg2.connect(
                user = self.login,
                password = self.password,
                host = self.host,
                port = self.port,
                # database = 'postgres'
            )
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = connection.cursor()
            sql_create_db = 'CREATE DATABASE vacancys;'
            cursor.execute(sql_create_db)
        except (Exception, psycopg2.Error) as error:
            print("Что-то пошло не так", error)
        finally:
            if connection:
                cursor.close()
                connection.close

    def create_table(self):
        try:
            connection = psycopg2.connect(
                user = self.login,
                password = self.password,
                host = self.host,
                port = self.port,
                database = 'vacancys'
            )
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = connection.cursor()
            for table in self.tables:
                sql_create_db = table
                cursor.execute(sql_create_db)
        except (Exception, psycopg2.Error) as error:
            print("Что-то пошло не так", error)
        finally:
            if connection:
                cursor.close()
                connection.close

db = PostgresWorker()
# db.create_database()
db.create_table()
