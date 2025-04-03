from repository.recordRepository import RecordRepository
from repository.rateRepository import RateRepository
from model.conversationDto import ConversationDto
from model.recordDto import RecordDto
from model.rateDto import RateDto
from service.rateService import RateService
from datetime import datetime

class ConversationService():
    def __init__(self):
        self.record_repository = RecordRepository()
        self.rate_repository = RateRepository()
        self.rate_service = RateService()

    def  get_all_conversations(self, page_index: int, page_size: int) -> list[ConversationDto]:
        if (page_index == None):
            page_index = 1
        if (page_size == None):
            page_size = 100

        offset = (page_index - 1) * page_size
        result = self.record_repository.get_all_conversation(offset, page_size)

        conversations = []
        for x in result:
            cnvr = ConversationDto(
                id=x[0],
                avg_csat=x[3],
                first_input=x[1],
                first_output=x[2],
                start_time=x[4],
                is_rated=x[5]
            )
            conversations.append(cnvr)
        return conversations
    
    
    def  get_total_count_conversation(self) -> int:    
        print("get_total_count")
        total = self.record_repository.get_total_count_conversation()
        return total[0][0]

    def get_conversation_by_id(self, conversation_id) -> ConversationDto:
        records = self.record_repository.get_record_by_conversation_id(conversation_id)
        
        recordDtos: list[RecordDto] = []
        for x in records:
            re = RecordDto(
                record_id=x[0],
                conversation_id=x[1],
                main_input=x[2],
                main_output=x[3],
                start_time=x[4],
                is_rated=x[5],
                rate=RateDto(
                    id=x[6],
                    record_id=x[0],
                    conversation_id=x[1],
                    csat=x[7],
                    groundedness=x[8],
                    answer_relevance=x[9],
                    context_relevance=x[10],
                    sentiment=x[11]
                )
            )
            recordDtos.append(re)
        
        first_record = records[0]
        conversation = ConversationDto(
            id=first_record[1],
            first_input=first_record[2],
            first_output=first_record[3],
            start_time=first_record[4],
            avg_csat=self.calculate_avg_csat(recordDtos),
            records=recordDtos
        )
        return conversation
    
    def calculate_avg_csat(self, records: list[RecordDto]):
        if (len(records) == 0): return 0
        sum = 0
        number_of_object = 0
        for x in records:
            if x.is_rated:
                sum = sum + x.rate.csat
                number_of_object += 1
        
        if (number_of_object == 0): return 0
        return sum / number_of_object
    
