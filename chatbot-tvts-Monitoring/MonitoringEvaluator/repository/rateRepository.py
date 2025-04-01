
import psycopg2
import json
from datetime import datetime
from repository.databaseUtils import get_database_params
from model.rateDto import RateDto

class RateRepository:
    def __init__(self):
        self.database_name = "DB_NAME_EVALUATION"
        self.conn = psycopg2.connect(**get_database_params(self.database_name))
        self.init_db()

    def init_db(self):
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS rates (
            id uuid primary key,
            record_id uuid,
            conversation_id uuid,
            csat decimal(4, 2) DEFAULT 0.00,
            groundedness decimal(4, 2) DEFAULT 0.00,
            answer_relevance decimal(4, 2) DEFAULT 0.00,
            context_relevance decimal(4, 2) DEFAULT 0.00,
            sentiment decimal(4, 2) DEFAULT 0.00,
            created_date TIMESTAMP,
            CONSTRAINT fk_rates_records foreign key(record_id) references records(id)
        )
        """)
        self.conn.commit()
        cur.close()
        self.conn.close()
    
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
    
    def create_rates(self, rateDtos: list[RateDto]):
        command = f"""
                INSERT INTO rates (id, record_Id, conversation_id, csat, groundedness, answer_relevance, context_relevance, sentiment, created_date)
                VALUES 
                """
        for idx, x in enumerate(rateDtos):
            if (idx == len(rateDtos) - 1):
                command += f"('{x.id}', '{x.record_id}', '{x.conversation_id}', {x.csat}, {x.groundedness}, {x.answer_relevance}, {x.context_relevance}, {x.sentiment}, '{datetime.now()}')"
            else:
                command += f"('{x.id}', '{x.record_id}', '{x.conversation_id}', {x.csat}, {x.groundedness}, {x.answer_relevance}, {x.context_relevance}, {x.sentiment}, '{datetime.now()}'),"

        self.command_database(command)
        return

    def get_all_rates(self):
        query = "SELECT * FROM rates"
        return self.query_database(query)
    
    def get_rate_by_record_id(self, record_id):
        query = f"SELECT * FROM rates where record_id='{record_id}'"
        return self.query_database(query)
    
    def get_rate_by_conversation_id(self, conversation_id):
        query = f"SELECT * FROM rates where conversation_id='{conversation_id}'"
        return self.query_database(query)


