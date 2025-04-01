from model.recordDto import RecordDto

class ConversationDto:
    def __init__(self, id=None, avg_csat=None, first_input=None, first_output=None, start_time=None, is_rated=False, records: list[RecordDto]=[]):
        self.id = id
        self.avg_csat = avg_csat    
        self.first_input = first_input
        self.first_output = first_output    
        self.start_time = start_time
        self.is_rated = is_rated
        self.records = records
    
    def __dict__(self):
        return dict(
            id = self.id,
            avg_csat = self.avg_csat,
            first_input = self.first_input,
            first_output = self.first_output,
            start_time = self.start_time,
            is_rated = self.is_rated,
            records = [x.__dict__() for x in self.records],
        )