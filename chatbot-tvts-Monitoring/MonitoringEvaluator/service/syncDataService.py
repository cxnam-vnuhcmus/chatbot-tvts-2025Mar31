from repository.recordRepository import RecordRepository
from repository.rateRepository import RateRepository
from repository.dialogueRepository import DialogueRepository
from model.createRecordDto import CreateRecordDto
from service.rateService import RateService
from model.rateDto import RateDto
from model.recordDto import RecordDto
from shared.constant.callName import CallName
from datetime import datetime
from shared.constant.datetimeFormat import DatetimeFormat
from shared.utils.utils import truncate_string
import uuid 

class SyncDataService():
    def __init__(self):
        self.record_repository = RecordRepository()
        self.rate_repository = RateRepository()
        self.dialogue_epository = DialogueRepository()
        self.rate_service = RateService()
    
    # This function will get all dialogues in a specific date (today by default) and save to table record
    # This function should be call every 24h
    def sync_new_dialogues_to_records(self, date=datetime.today()):
        print("[{date}] | SYNCING DATA | Syncing date: {syncing_date}".format(date=datetime.today(), syncing_date=date.strftime(DatetimeFormat.YYYYMMDD)))
        print("[{date}] | SYNCING DATA | Start".format(date=datetime.today()))

        # Get all dialogues in date
        result = self.dialogue_epository.get_dialogue_by_date(date)
        print("[{date}] | SYNCING DATA | Found {dialogues} new dialogues".format(date=datetime.today(), dialogues=len(result)))

        # Mapping dialogues to records
        records: list[CreateRecordDto] = [self.map_dialogues_to_records(x) for x in result]

        # Get exist records in date
        existed_records = self.record_repository.get_record_by_date(date)
        existed_records_ids = [record[0] for record in existed_records]
        
        # Get new records
        new_records = [record for record in records if record.id not in existed_records_ids]
        print("[{date}] | SYNCING DATA | Found {records} new records to sync".format(date=datetime.today(), records=len(new_records)))
        
        # Save records to database
        if (len(new_records) > 0):
            print("[{date}] | SYNCING DATA | Syncing...".format(date=datetime.today()))
            self.record_repository.create_records(records)

        print("[{date}] | SYNCING DATA | END".format(date=datetime.today()))

        return result

    
    def map_dialogues_to_records(self, dialogue) -> list[CreateRecordDto]:
        # Get raw data
        dialogue_id = dialogue[0]
        record_id = dialogue[1]
        conversation_id = dialogue[2]
        app_id = dialogue[3]
        main_input = dialogue[4]
        main_output = dialogue[5]
        main_error = dialogue[6]
        perf = dialogue[7]
        calls = dialogue[8]
        created_at = dialogue[9]

        # Manipulate data
        #main_input = truncate_string(main_input, 250) # only need to store first 255 character
        #main_output = truncate_string(main_output, 250) # only need to store first 255 character
        record_data = {
            "record_id": record_id,
            "conversation_id": conversation_id,
            "user_id": None,
            "user_name": None,
            "app_id": app_id,
            "perf": perf,
            "main_input": main_input,
            "main_output": main_output,
            "main_error": main_error,
            "calls": calls
        }

        createRecordDto = CreateRecordDto(
            record_id, 
            conversation_id, 
            record_data, 
            main_input, 
            main_output, 
            perf["start_time"])

        return createRecordDto


