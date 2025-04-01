
import psycopg2
import json
from datetime import datetime, timedelta
from repository.databaseUtils import get_database_params
from model.rateDto import RateDto
from shared.constant.datetimeFormat import DatetimeFormat

class DialogueRepository:
    def __init__(self):
        self.database_name = "DB_NAME_CHATBOT"
        self.conn = psycopg2.connect(**get_database_params(self.database_name))
    
    def query_database(self, query):
        self.conn = psycopg2.connect(**get_database_params(self.database_name))
        cur = self.conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        self.conn.commit()
        cur.close()
        self.conn.close()
        return rows

    def command_database(self, command):
        self.conn = psycopg2.connect(**get_database_params(self.database_name))
        cur = self.conn.cursor()
        cur.execute(command)
        self.conn.commit()
        cur.close()
        self.conn.close() 
    
    def get_dialogue_to_rate(self):
        query = "SELECT * FROM dialogues limit 5"
        return self.query_database(query)
    
    def get_dialogue_by_date(self, date: datetime):
        date_from = date.strftime(DatetimeFormat.YYYYMMDD)
        date_to = (date + timedelta(days=1)).strftime(DatetimeFormat.YYYYMMDD)
        query = """SELECT * FROM dialogues  
                WHERE CAST('{date_from}' AS DATE) < created_at 
                and created_at < CAST('{date_to}' AS DATE)""".format(date_from = date_from, date_to = date_to)
        
        return self.query_database(query)
    
    def get_dialogue_record_id_by_date(self, date: datetime):
        date_from = date.strftime(DatetimeFormat.YYYYMMDD)
        date_to = (date + timedelta(days=1)).strftime(DatetimeFormat.YYYYMMDD)
        query = """SELECT record_id FROM dialogues  
                WHERE CAST('{date_from}' AS DATE) < created_at 
                and created_at < CAST('{date_to}' AS DATE)""".format(date_from = date_from, date_to = date_to)
        
        return self.query_database(query)


