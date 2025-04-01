class CreateRecordDto:
    def __init__(self, record_id, conversation_id, record_data, main_input, main_output, start_time):
        self.id = record_id
        self.conversation_id = conversation_id
        self.record_data = record_data
        self.main_input = main_input
        self.main_output = main_output
        self.start_time = start_time