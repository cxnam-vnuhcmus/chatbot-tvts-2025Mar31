from model.rateDto import RateDto

class RecordDto:
    def __init__(self, record_id=None, conversation_id=None, record_data=None, main_input=None, main_output=None, start_time=None, is_rated=None, created_date=None, rate: RateDto=None):
        self.id = record_id
        self.conversation_id = conversation_id
        self.record_data = record_data        
        self.main_input = main_input
        self.main_output = main_output
        self.start_time = start_time
        self.is_rated = is_rated    
        self.created_date = created_date
        self.rate = rate

    def __dict__(self):
        return dict(
            id = self.id,
            conversation_id = self.conversation_id,
            record_data = self.record_data,
            main_input = self.main_input,
            main_output = self.main_output,
            start_time = self.start_time,
            is_rated = self.is_rated,
            created_date = self.created_date,
            rate = self.rate.__dict__(),
        )