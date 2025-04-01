from repository.recordRepository import RecordRepository
from repository.rateRepository import RateRepository
from model.createRecordDto import CreateRecordDto
from service.rateService import RateService
from model.rateDto import RateDto
from shared.constant.callName import CallName
from datetime import datetime
import uuid 

class RecordService():
    def __init__(self):
        self.record_repository = RecordRepository()
        self.rate_repository = RateRepository()
        self.rate_service = RateService()

    def create_records(self, records):
        createRecords = []
        for record in records:
            createRecordDto = CreateRecordDto(record["record_id"], record["conversation_id"], record, record["main_input"], record["main_output"], record["perf"]["start_time"])
            createRecords.append(createRecordDto)

        self.record_repository.create_records(createRecords)
        return
    
    # This function will get all dialogues in a specific date (today by default) and save to table record
    # This function should be call every 24h
    def sync_new_dialogues_to_records(self, date=datetime.today()):
        # Get top 
        return
    

    def rate_by_record_id(self, record_id: str) -> RateDto:
        result = self.record_repository.get_record_by_id(record_id)

        # Check exist
        if len(result) == 0:
            raise Exception(f"Record not found: id = {record_id}")

        # rate record
        record_data = result[0][2]
        rate = self.rate_record(record_data)

        # save to table rate
        self.rate_repository.create_rates([rate])

        # update table record
        self.record_repository.update_is_rated([rate.record_id])

        return rate


    def rate_all_records(self, date: datetime = None) -> list[list[RateDto], int, int, int]:
        print("[{date}] | RATING DATA | Start".format(date=datetime.today()))

        records = self.record_repository.get_records_to_rate(date)
        total = len(records)
        totalSuccess = 0
        totalFailure = 0
        print("[{date}] | RATING DATA | Found {total} to rate".format(date=datetime.today(), total=total))

        if (total == 0):
            print("[{date}] | RATING DATA | End".format(date=datetime.today(), total=total))
            return [[], total, totalSuccess, totalFailure]

        rates = []
        for i, record in enumerate(records) :
            try:
                # rate record
                print("[{date}] | RATING DATA | Rating ({index}/{total})...".format(date=datetime.today(), index=i+1, total=total))
                record_data = record[2]
                rate = self.rate_record(record_data)
                totalSuccess+=1
                rates.append(rate)
            except:
                print("[{date}] | RATING DATA | Error when rating Record: [{record_id}]".format(date=datetime.today(), record_id=record[0]))
                totalFailure+=1
         
        if len(rates) > 0:
            print("[{date}] | RATING DATA | Saving to database...".format(date=datetime.today()))
            # save to table rate
            self.rate_repository.create_rates(rates)
            # update table record
            self.record_repository.update_is_rated([x.record_id for x in rates])
        else:
            print("[{date}] | RATING DATA | No new data to saving to database".format(date=datetime.today()))

        print("[{date}] | RATING DATA | End".format(date=datetime.today()))

        return [rates, total, totalSuccess, totalFailure]
    
    def rate_record(self, record_data) -> RateDto:
        record_id = record_data["record_id"]
        conversation_id = record_data["conversation_id"]

        call_get_intent = self.find_call(record_data, CallName.Chatbot_Task_GetIntent)
        call_get_search_term = self.find_call(record_data, CallName.Chatbot_Task_GetSearchTerm)
        call_get_related_documents = self.find_call(record_data, CallName.Chatbot_Task_GetRelatedDocuments)
        call_get_answer = self.find_call(record_data, CallName.Chatbot_Task_GetAnswer)
        call_get_follow_up_questions = self.find_call(record_data, CallName.Chatbot_Task_GetFollowUpQuestions)

        question = record_data["main_input"]
        answer = record_data["main_output"]
        histories = call_get_intent["args"]["histories"]
        intent = call_get_intent["rets"]["intent"]
        search_terms = call_get_search_term["rets"]["search_terms"]
        documents = call_get_related_documents["rets"]["documents"]
        #followUpQuestions = call_get_follow_up_questions["rets"]["questions"]

        searchTermString = ', '.join(search_terms)    
        historyString = ', '.join(histories)
        documentString = ', '.join(documents)
        #followUpQuestionString = ', '.join(followUpQuestions)

        answer_relevance = self.rate_service.get_answer_relevance(question=question, answer=answer)
        context_relevance = self.rate_service.get_context_relevance(question=searchTermString, context=documentString)
        groundedness = self.rate_service.get_groundedness(answer=answer, context=documentString)
        sentiment = self.rate_service.get_sentiment(histories=historyString, question=question, answer=answer)

        csat = (answer_relevance*30 + context_relevance*20 + groundedness*35 + sentiment*15) / 100

        rate = RateDto(
            id=uuid.uuid4(),
            record_id=record_id,
            conversation_id=conversation_id,
            csat=csat,
            groundedness=groundedness,
            context_relevance=context_relevance,
            answer_relevance=answer_relevance,
            sentiment=sentiment,
            created_date=None
        )
        return rate

    def find_call(self, record_data, call_name):
        for call in record_data["calls"]:
            if (call["call_name"] == call_name):
                return call
        return None
    
