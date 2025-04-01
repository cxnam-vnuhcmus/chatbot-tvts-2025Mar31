from model.rateDto import RateDto
from model.perfDto import PerfDto

class DialogueDto:
    def __init__(self, record_id=None, conversation_id=None, app_id=None, main_input=None, main_output=None, main_error=None, perf: PerfDto=None, calls=None):
        self.id = record_id
        self.conversation_id = conversation_id
        self.app_id = app_id        
        self.main_input = main_input
        self.main_output = main_output
        self.main_error = main_error
        self.perf = perf    
        self.calls = calls

    def __dict__(self):
        return dict(
            id = self.id,
            conversation_id = self.conversation_id,
            app_id = self.app_id,
            record_data = self.record_data,
            main_input = self.main_input,
            main_output = self.main_output,
            main_error = self.main_error,
            perf = self.perf.__dict__(),
            calls = self.calls,
        )