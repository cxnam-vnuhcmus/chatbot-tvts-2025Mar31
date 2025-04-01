
import psycopg2
import json
from datetime import datetime, timedelta
from repository.databaseUtils import get_database_params
from model.createRecordDto import CreateRecordDto
from shared.constant.datetimeFormat import DatetimeFormat

class RecordRepository:
    def __init__(self):
        self.database_name = "DB_NAME_EVALUATION"
        # print(**get_database_params("DB_NAME_EVALUATION"))
        self.conn = psycopg2.connect(**get_database_params("DB_NAME_EVALUATION"))
        self.init_db()

    def init_db(self):
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS records (
            id uuid primary key,
            conversation_id uuid,
            record_data json,
            main_input text,
            main_output text,
            start_time TIMESTAMP,
            is_rated bool default false,
            created_date TIMESTAMP
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

    
    def command_database_many(self, command, parameters=None):
        self.conn = psycopg2.connect(**get_database_params(self.database_name))
        cur = self.conn.cursor()
        cur.executemany(command, parameters)
        self.conn.commit()
        cur.close()
        self.conn.close()   

    def create_records(self, recordDtos: list[CreateRecordDto]):
        command = """INSERT INTO records (id, conversation_id, record_data, main_input, main_output, start_time, created_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        parameters = [
            (
                x.id, 
                x.conversation_id, 
                json.dumps(x.record_data), 
                x.main_input, 
                x.main_output, 
                x.start_time, 
                datetime.now()
            ) for x in recordDtos
        ]

        self.command_database_many(command, parameters)
        return
    
    def update_is_rated(self, record_ids: list[str]):
        command = "UPDATE records SET is_rated=true where"
        for idx, x in enumerate(record_ids):
            if (idx == len(record_ids) - 1):
                command += f" id = '{x}'"
            else:
                command += f" id = '{x}' or"
        self.command_database(command)
        return

    def get_all_records(self):
        query = "SELECT * FROM records"
        return self.query_database(query)
    
    def get_record_by_id(self, record_id):
        query = f"SELECT * FROM records where id='{record_id}'"
        return self.query_database(query)
    
    def get_record_by_conversation_id(self, conversation_id):
        query = ("select re.id, re.conversation_id, re.main_input, re.main_output, re.start_time, re.is_rated, ra.id, ra.csat, ra.groundedness, ra.answer_relevance, ra.context_relevance, ra.sentiment "
                "from records re left join rates ra on re.id = ra.record_id "
                f"where re.conversation_id = '{conversation_id}' "
                "order by re.start_time")
        return self.query_database(query)
    
    def get_records_to_rate(self, date: datetime = None):
        if (date == None):
            query = "SELECT id, conversation_id, record_data FROM records where is_rated = false"
        else:
            date_from = date.strftime(DatetimeFormat.YYYYMMDD)
            date_to = (date + timedelta(days=1)).strftime(DatetimeFormat.YYYYMMDD)
            query = """SELECT id, conversation_id, record_data FROM records 
                where is_rated = false 
                and CAST('{date_from}' AS DATE) <= start_time 
                and start_time <= CAST('{date_to}' AS DATE)""".format(date_from = date_from, date_to = date_to)
        return self.query_database(query)
    
    def get_record_by_date(self, date: datetime):
        date_from = date.strftime(DatetimeFormat.YYYYMMDD)
        date_to = (date + timedelta(days=1)).strftime(DatetimeFormat.YYYYMMDD)
        query = """SELECT id FROM records  
                WHERE CAST('{date_from}' AS DATE) < start_time 
                and start_time < CAST('{date_to}' AS DATE)""".format(date_from = date_from, date_to = date_to)
        
        return self.query_database(query)
    
    def get_all_conversation(self, offset=0, limit=100):
        query = ("""select re2.conversation_id, re2.main_input, re2.main_output, cnvr.avg_csat, re2.start_time, re2.is_rated 
                    from
                        (select re.conversation_id, min(re.start_time) as min_start_time, avg(ra.csat) as avg_csat 
                        from records re join rates ra on re.id = ra.record_id
                        group by re.conversation_id) cnvr
                        right join 
                        (select *
                        from (select *, row_number() over (partition by conversation_id order by start_time) as rn
                            from records) r
                        where rn=1) re2
                        on cnvr.min_start_time=re2.start_time and cnvr.conversation_id=re2.conversation_id 
                        order by re2.start_time desc """
                    f"LIMIT {limit} OFFSET {offset}")
        return self.query_database(query)

    def get_total_count_conversation(self):
        query = ("select count(distinct(r.conversation_id)) from records r")
        return self.query_database(query)

