from repository.recordRepository import RecordRepository
from repository.rateRepository import RateRepository
from model.createRecordDto import CreateRecordDto
from service.rateService import RateService
from model.rateDto import RateDto
from shared.constant.callName import CallName
from datetime import datetime
import uuid 

class DialoguesService():
    def __init__(self):
        self.record_repository = RecordRepository()
        self.rate_repository = RateRepository()
        self.rate_service = RateService()

    
    # This function will get all dialogues in today by default and save to table record
    # This function should be call every 24h
    def sync_new_dialogues(self, date=datetime.today()):
        # Get top 
        return

    
