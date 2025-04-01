from datetime import datetime, timezone
import uuid
from flask import Flask, request, jsonify, Response
import json


from ChatbotAgent.bot import ChatCommand, ChatbotResponse, get_chatbot_instance
from config import CHATBOT_AGENT_PORT
from ChatbotAgent.v1.commands import database_cli
from models import get_session, Session, Dialogue, Feedback, CSATEnum
import sqlalchemy as db

app = Flask(__name__)

app.cli.add_command(database_cli)


@app.post('/completion')
def output():
    c = get_chatbot_instance()
    stream = request.args.get("stream")
    body = json.loads(request.data)
    session_id = body.get("session_id")
    question = body.get("msg")
    if not session_id:
        session_id = str(uuid.uuid4())
    chat_gen = c.ask(question, session_id=session_id)

    if stream:
        def generate():
            while True:
                try:
                    cmd, msg = next(chat_gen)
                    # print(f"cmd: {cmd} msg: {msg}")
                    if cmd == ChatCommand.ANSWERING:
                        if preCmd == ChatCommand.BEGIN_ANSWER.name:
                            yield json.dumps({"event": ChatCommand.BEGIN_ANSWER.name, "data": "", "session_id": session_id}) + "\n\n"
                        yield json.dumps({"event": ChatCommand.ANSWERING.name, "data": msg, "session_id": session_id}) + "\n\n"
                    elif cmd != ChatCommand.END_ANSWER:
                        yield json.dumps({"event": cmd.name, "data": msg, "session_id": session_id}) + "\n\n"
                    elif cmd == ChatCommand.END_ANSWER:
                        print(f"cmd: {cmd} msg: {msg}")

                    preCmd = cmd
                except StopIteration as e:
                    returned = e.value
                    return returned
        return Response(generate(), mimetype='text/event-stream')
    else:
        def generate():
            while True:
                try:
                    cmd, msg = next(chat_gen)
                    print(f"cmd: {cmd} msg: {msg}")
                except StopIteration as e:
                    returned = e.value
                    return returned
        res = generate()
        return jsonify({
            "question": res.question,
            "answer": res.answer,
            "session_id": session_id
            # "followup_questions": res.followup_questions
        })


@app.get('/')
def hello_world():
    return 'Running'


@app.get("/conversations/<session_id>")
def get_conversations(session_id: str):
    histories = []
    with get_session().connect() as conn:
        for row in conn.execute(db.select(Session).where(Session.c.session_id == session_id).order_by(Session.c.created_at.desc())).fetchmany(10):
            histories.append({
                "role": row.role.value,
                "content": row.content,
                "created_at": row.created_at.timestamp()
            })
    return jsonify(histories)


@app.get("/logs/<session_id>")
def get_logs(session_id: str):
    logs = []
    with get_session().connect() as conn:
        for row in conn.execute(
            db.select(Dialogue).where(Dialogue.c.conversation_id ==
                                      session_id).order_by(Dialogue.c.created_at.asc())
        ):
            logs.append(dict(row._mapping))

    return jsonify(logs)


@app.get("/sessions")
def get_sessions():
    sessions = []
    with get_session().connect() as conn:
        for row in conn.execute(db.select(Session.c.session_id, db.func.max(Session.c.created_at).label("latest_created_at"),
                                          ).where(Session.c.session_id != None).group_by(Session.c.session_id).order_by(db.func.max(Session.c.created_at).desc())).fetchall():
            sessions.append(dict(row._mapping))

        session_ids = [session["session_id"] for session in sessions]
        subq = db.select(Session).where(Session.c.session_id == Session.c.session_id).order_by(
            Session.c.session_id).limit(2).correlate(Session)
        res = []
        for row in conn.execute(db.select(Session).where(Session.c.session_id.in_(session_ids), db.exists(subq)).order_by(Session.c.session_id, Session.c.created_at)).fetchall():
            res.append(dict(row._mapping))

        for sess in sessions:
            session_id = sess['session_id']
            session_details = list(map(
                lambda e: {"role": e['role'], "content": e['content']}, filter(lambda e: e['session_id'] == session_id, res)))
            sess['session_details'] = session_details

    return jsonify(sessions)


@app.post("/feedbacks")
def post_feedback():
    body = json.loads(request.data)
    with get_session().connect() as conn:
        session_id = body.get("session_id")
        sessions = []
        now = datetime.now(timezone.utc)
        for row in conn.execute(db.select(Session).where(Session.c.session_id == session_id, Session.c.created_at <= now).order_by(Session.c.created_at.asc())).fetchall():
            sessions.append(
                {**row._mapping, "session_id": session_id,
                    "created_at": row.created_at.timestamp()}
            )
        conn.execute(
            db.insert(Feedback).values({
                "session_id": session_id,
                "content": body.get("content"),
                "rating": CSATEnum(body.get("rating")),
                "conversations": sessions
            })
        )
        conn.commit()

    return jsonify({"status": "success"})


@app.get("/feedbacks/<session_id>")
def get_feedbacks_by_session_id(session_id: str):
    with get_session().connect() as conn:
        feedbacks = []
        for row in conn.execute(db.select(Feedback).where(Feedback.c.session_id == session_id).order_by(Feedback.c.created_at.desc())).fetchall():
            feedbacks.append(dict(row._mapping))
        return jsonify(feedbacks)


if __name__ == '__main__':
    port = int(CHATBOT_AGENT_PORT)
    app.run(debug=True, port=port)
