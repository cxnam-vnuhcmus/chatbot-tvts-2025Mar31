import threading
from flask import Flask, request, jsonify
import os
import jsonpickle
from typing import List
from dotenv import load_dotenv
from prompt.promptDB import PromptDB
from service.recordService import RecordService
from service.conversationService import ConversationService
from repository.dialogueRepository import DialogueRepository
from service.syncDataService import SyncDataService
from datetime import datetime
import json

from shared.constant.datetimeFormat import DatetimeFormat
from shared.utils.utils import obj_dict
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
import time

load_dotenv()

app = Flask(__name__)
CORS(app)  # Allows all origins by default

record_service = RecordService()
conversation_service = ConversationService()
dialogue_repository = DialogueRepository()
sync_data_service = SyncDataService()


@app.route('/version', methods=['GET'])
def version():
    return jsonify(int(os.getenv('MonitoringEvaluator_VERSION')))


@app.route('/prompts', methods=['GET'])
def get_prompts():
    promptDB = PromptDB()
    return jsonify(promptDB.get_list_prompts())

#
# API Records
#

@app.route('/records', methods=['POST'])
def create_records():
    data = request.get_json()
    records = data["records"]
    record_service.create_records(records)
    return jsonify({'message': 'Ok'}), 200

@app.route('/records/<string:record_id>/rate', methods=['GET'])
def rate_record_by_id(record_id):
    result = record_service.rate_by_record_id(record_id)
    return json.dumps(result.__dict__(), default=str), 200

@app.route('/records/rate-all', methods=['POST'])
def rate_all_records():
    data = request.get_json()
    date_str = data.get("date", None)

    # default date: today
    if (date_str == None):
        result = record_service.rate_all_records()
    else:
        date = datetime.strptime(date_str, DatetimeFormat.YYYYMMDD)
        result = record_service.rate_all_records(date)

    rates = json.dumps([x.__dict__() for x in result[0]], default=str)
    total = result[1]
    totalSuccess = result[2]
    totalFailure = result[3]
    return jsonify({"total": total, "totalSuccess": totalSuccess, "totalFailure": totalFailure, "rates": rates}), 200

#
# API Dialogues
#

@app.route('/dialogues/get-all', methods=['GET'])
def get_dialogues():
    result = dialogue_repository.get_dialogue_to_rate()
    return result, 200

@app.route('/dialogues/sync-data', methods=['POST'])
def sync_dialogues():
    data = request.get_json()
    date_str = data.get("date", None)

    # default date: today
    if (date_str is None):
        date_str = datetime.today().strftime(DatetimeFormat.YYYYMMDD)

    date = datetime.strptime(date_str, DatetimeFormat.YYYYMMDD)
    result = sync_data_service.sync_new_dialogues_to_records(date)
    return jsonify({"total": len(result), "data": result}), 200

#
# API Conversations
#

@app.route('/conversations', methods=['GET'])
def get_all_conversions():
    try:
        page_index = int(request.args.get('pageIndex'))
        page_size = int(request.args.get('pageSize'))
    
        result = conversation_service.get_all_conversations(page_index, page_size)
        return json.dumps([x.__dict__() for x in result], default=str), 200
    except Exception as e:
        return jsonify({"message": str(e)}), 500

@app.route('/conversations/count', methods=['GET'])
def get_total_count_conversion():
    try:
        result = conversation_service.get_total_count_conversation()
        return { "total": result }, 200
    except Exception as e:
        return jsonify({"message": str(e)}), 500

@app.route('/conversations/<string:conversation_id>', methods=['GET'])
def get_conversion_by_id(conversation_id):
    try:
        result = conversation_service.get_conversation_by_id(conversation_id)
        return json.dumps(result.__dict__(), default=str), 200
    except Exception as e:
        return jsonify({"message": str(e)}), 500

#
# Scheduling function
#

def monitoring_evaluator_scheduler():
    scheduler_thread = threading.Thread(target=sync_and_rate_dialogues, args=(datetime.now(),))
    scheduler_thread.start()
    return

# This function will sync and rate dialogues in today
def sync_and_rate_dialogues(date: datetime):
    new_dialogues = sync_data_service.sync_new_dialogues_to_records(date)
    result = record_service.rate_all_records(date)
    return 

### Add scheduler to sync dialogue data
scheduler = BackgroundScheduler()
interval_seconds = 86400 # 86400 second = 24 hour
scheduler.add_job(monitoring_evaluator_scheduler, 'interval', seconds=interval_seconds) 
scheduler.start()

if __name__ == '__main__':
    port = int(os.getenv('MonitoringEvaluator_PORT', 6821))
    app.run(host="0.0.0.0", debug=True, port=port, use_reloader=False)
