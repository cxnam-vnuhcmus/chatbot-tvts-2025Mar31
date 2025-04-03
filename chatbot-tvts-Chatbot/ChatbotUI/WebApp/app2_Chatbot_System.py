from datetime import datetime, date
import os
import sys
from textwrap import dedent
from flask import json
from numpy import pi
import pandas as pd
import panel as pn
import numpy as np
import requests
import param
from bokeh.io import curdoc
from bokeh.palettes import Category10_10
from bokeh.plotting import figure
from bokeh.transform import cumsum
import asyncio
from bokeh.models.widgets.tables import HTMLTemplateFormatter


sys.path.append(os.path.abspath("."))

doc = curdoc()
from config import API_URL
app = None
logs = None

pn.extension('tabulator', css_files=[
    "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.2/css/all.min.css"], notifications=True)
pn.state.notifications.position = 'top-right'


sys.path.append(os.path.abspath("."))

pn.extension()

# setting query params


class Settings(param.Parameterized):
    session_id = param.String(default=None)


settings = Settings()
session_input = pn.widgets.TextInput(value="")
pn.state.location.sync(settings, ["session_id"])
session_settings = pn.Param(settings, parameters=["session_id"], widgets={
    "session_id": session_input})


def request_chatbot(session_id: str, input: str):
    print("request session_id", session_id)
    url = f"{API_URL}/completion?stream=1"

    payload = json.dumps({
        "session_id": str(session_id),
        "msg": input
    })

    headers = {
        'Content-Type': 'application/json'
    }
    with requests.post(
        url,
        headers=headers,
        data=payload,
        stream=True
    ) as response:
        if response.status_code != 200:
            response.raise_for_status()
            print(f"Error: {response.status_code}")
            return
        for line in response.iter_lines():
            if line:  # Skip empty lines
                payload = json.loads(line)
                yield payload.get("event"), payload.get("data"), payload.get("session_id")


async def response_callback(
    input_message: str, input_user: str, instance: pn.chat.ChatInterface
):
    # choose your favorite LLM API to respond to the input_message
    ...
    # histories = list(
    #     map(
    #         lambda c: History(role=c.get("role", ""), content=c.get("content", "")),
    #         chatPanel.serialize(),
    #     )
    # )
    q1_btn.disabled = True
    q2_btn.disabled = True
    q3_btn.disabled = True

    input = input_message
    if q1_btn.name == input_message:
        # QUESTION_ID
        input = q1_btn.name
    elif q2_btn.name == input_message:
        input = q2_btn.name
    elif q3_btn.name == input_message:
        input = q3_btn.name

    # chat_gen = c.ask(input, histories)
    chat_gen = request_chatbot(session_input.value, input)
    # chatMessage = pn.chat.ChatMessage(pn.pane.Markdown(
    #     "Received message...."), user="Assistant", avatar="🤖")
    # chatPanel.append(chatMessage)
    msg_output = ""
    session_id = ""
    preCmd = None
    while True:
        try:
            cmd, msg, session_id = next(chat_gen)
            print(f"cmd: {cmd} msg: {msg}, session_id: {session_id}")
            if cmd == "ANSWERING":
                if preCmd == "BEGIN_ANSWER":
                    # chatMessage.object = pn.pane.Markdown("")
                    msg_output = ""
                # chatMessage.object = pn.pane.Markdown(
                #    chatMessage.object.object + msg)
                msg_output += msg
            elif cmd == "FOLLOWUP_QUESTIONS":
                questions = msg.split("<|>")
                q1_btn.name = questions[0]
                q2_btn.name = questions[1]
                q3_btn.name = questions[2]
            elif cmd != "END_ANSWER":
                # chatMessage.object = pn.pane.Markdown(msg)
                msg_output += f"\n{msg}"

            preCmd = cmd
        except StopIteration as e:
            returned = e.value

            q1_btn.disabled = False
            q2_btn.disabled = False
            q3_btn.disabled = False

            break

        yield msg_output
        await asyncio.sleep(0.1)

    session_input.value = session_id

session_value = pn.widgets.TextInput(value='')

q1_btn = pn.widgets.Button(name="", disabled=True)
q2_btn = pn.widgets.Button(name="", disabled=True)
q3_btn = pn.widgets.Button(name="", disabled=True)

chatPanel = pn.chat.ChatInterface(
    callback=response_callback,
    widgets=[
        pn.chat.ChatAreaInput(name="Câu hỏi"),
    ],
    show_rerun=False,
    show_undo=False,
    show_clear=False,
)


def get_conversations(session_id: str):
    url = f"{API_URL}/conversations/{session_id}"
    response = requests.get(url)
    return response.json()


def get_logs(session_id):
    url = f"{API_URL}/logs/{session_id}"
    response = requests.get(url)
    return response.json()


if pn.state.location.query_params.get("session_id"):
    session_id = pn.state.location.query_params.get("session_id")
    session_input.value = session_id
    results = get_conversations(session_id)
    results = sorted(results, key=lambda x: x['created_at'])
    for result in results:
        chatPanel.send(pn.chat.ChatMessage(result.get("content"), avatar="🤖" if result.get(
            "role") == "system" else "", timestamp=datetime.fromtimestamp(result.get("created_at"))), respond=False)

q1_btn.on_click(lambda event: chatPanel.send(str(f'{q1_btn.name}')))
q2_btn.on_click(lambda event: chatPanel.send(str(f'{q2_btn.name}')))
q3_btn.on_click(lambda event: chatPanel.send(str(f'{q3_btn.name}')))


# TODO: Dashbaord screen


styles = {
    "box-shadow": "rgba(50, 50, 93, 0.25) 0px 6px 12px -2px, rgba(0, 0, 0, 0.3) 0px 3px 7px -3px",
    "border-radius": "4px",
    "padding": "10px",
}


def _get_tokens(rets):
    try:
        return rets['completion']['usage']['total_tokens']
    except:
        return 0


def _calculate_tokens_and_price(log):
    total_tokens = sum(map(lambda e: e['total_tokens'], log.get("calls")))
    return total_tokens, _calculate_price(total_tokens)


def _calulate_tokens_by_command(log):
    commands = log.get("calls")
    labels = []
    values = []
    for cmd in commands:
        labels.append(cmd['call_name'])
        values.append(cmd['total_tokens'])
    return labels, values


def _calulate_data_frame(log):
    data = []
    for e in log.get("calls"):
        if e['call_name'] == "AnswerUsingStreamCommand":
            data.append(
                {
                    "call_name": e['call_name'],
                    "duration": f"{(datetime.fromisoformat(e['perf']['end_time']) - datetime.fromisoformat(e['perf']['start_time'])).total_seconds()}s",
                    "total_tokens": e['total_tokens'],
                    "prices": _calculate_price(e['total_tokens']),
                    "input": json.dumps(e['args']),
                    "output": json.dumps(e['rets']),
                }
            )
    return data


def _get_docs(log):
    try:
        log_call = list(filter(lambda e: e['call_name'] ==
                               'AnswerUsingStreamCommand', log['calls']))
        log_call = log_call[0] if len(log_call) else None

        docs = log_call['args']['docs']

        return docs if len(docs) else None
    except:
        return None


def render_detail(log):

    # detail
    text_detail = pn.pane.HTML(
        f"""
        <h2>Câu hỏi</h2>
        <p>{log['main_input']}</p>
        <h2>Câu trả lời</h2>
        <p>{log['main_output']}</p>
        <h2>Tổng thời gian thực hiện</h2>
        <p>{(datetime.fromisoformat(log['perf']['end_time']) - datetime.fromisoformat(log['perf']['start_time'])).total_seconds()}s</p>
        """,
        width=600,
    )
    total_tokens, total_prices = _calculate_tokens_and_price(log)

    indicators2 = pn.Row(
        pn.indicators.Number(
            value=total_prices, name="Tổng chi phí", format="${value:,.6f}", styles=styles,
        ),
        pn.indicators.Number(
            value=total_tokens,
            name="Tổng số lượng token",
            format="{value:,.0f}",
            styles=styles,
        ),
    )

    # bar chart

    x_range, y_range = _calulate_tokens_by_command(log)
    ind = np.nonzero(y_range)[0]

    labels = [x_range[i] for i in ind]
    values = [y_range[i] for i in ind]
    angles = [i / sum(values) * 2 * pi for i in values]
    colors = Category10_10[:len(values)]
    pie_data = pd.DataFrame(
        {'label': labels, 'value': values, 'angle': angles, 'color': colors})

    p = figure(height=350, title="Token Usage Distribution Pie Chart", toolbar_location=None,
               tools="hover", tooltips="@label: @value", x_range=(-0.5, 1.0))

    r = p.wedge(x=0, y=1, radius=0.3,
                start_angle=cumsum('angle', include_zero=True), end_angle=cumsum('angle'),
                line_color="white", fill_color='color', legend_field='label', source=pie_data)

    p.axis.axis_label = None
    p.axis.visible = False
    p.grid.grid_line_color = None

    # tabledata

    docs = _get_docs(log)

    if docs:
        text = pn.pane.HTML(
            f"""<h2>Các tài liệu sử dụng</h2>""",
        )
        df = pd.DataFrame(
            list(map(lambda e: {'documents': e}, docs)))
        table = pn.widgets.Tabulator(
            df, sizing_mode='stretch_width',
            layout='fit_columns', height=300,
            widths={
                'index': 80
            }
        )
        detail_dashboard = pn.Column(
            pn.Row(
                pn.Column(
                    pn.Row(indicators2),
                    pn.Row(text_detail),
                ),
                pn.Column(p),

            ), pn.Column(text, table)
        )
    else:
        detail_dashboard = pn.Column(
            pn.Row(
                pn.Column(
                    pn.Row(indicators2),
                    pn.Row(text_detail),
                ),
                pn.Column(p),
            )
        )
    return detail_dashboard


def _calculate_all_logs(logs):
    for log in logs:
        totals = 0
        commands = log.get("calls")
        for cmd in commands:
            if type(cmd['rets']) is list:
                total_tokens = 0
                for ret in cmd['rets']:
                    total_tokens = total_tokens + _get_tokens(ret)
                cmd['total_tokens'] = total_tokens
            else:
                cmd['total_tokens'] = _get_tokens(cmd['rets'])
            totals = totals + cmd['total_tokens']
        log['total_tokens'] = totals
    return logs


def _calculate_total_tokens_of_logs(logs):
    return sum(map(lambda e: e['total_tokens'], logs))


def _calculate_price(tokens):
    return (tokens * 0.150) / 1e6


def dashboard_refresher_func(event):
    if not event:
        return
    dashboard.objects = build_dashboard()


def get_feedbacks(session_id: str):
    url = f"{API_URL}/feedbacks/{session_id}"
    response = requests.get(url)
    return response.json()


dashboard_refresher = pn.widgets.Button(name="Refresh")
pn.bind(dashboard_refresher_func, dashboard_refresher, watch=True)


def render_feedbacks(feedbacks):
    if not len(feedbacks):
        return []
    df = pd.DataFrame(feedbacks)
    table = pn.widgets.Tabulator(
        df[['created_at', 'rating', 'content']
           ], sizing_mode='stretch_width',
        layout='fit_columns',
        widths={
            'index': 80,
            "content": 1000
        },
        formatters={
            "content": HTMLTemplateFormatter()
        },
        disabled=True,
        page_size=10,
        pagination='local',
    )

    return [
        pn.pane.Markdown("## Các phản hồi người dùng"),
        pn.Row(table, sizing_mode="stretch_width"),
    ]


def build_dashboard() -> list:
    # total dashboard
    if not session_input.value:
        return [dashboard_refresher]
    logs = get_logs(session_input.value)

    _calculate_all_logs(logs)

    # indication
    count = len(logs)
    total_tokens = _calculate_total_tokens_of_logs(logs)
    prices = _calculate_price(total_tokens)
    indicators_total_1 = pn.Row(
        pn.indicators.Number(
            value=count, name="Số lượng phản hồi", format="{value:,.0f}", styles=styles
        ),
        pn.indicators.Number(
            value=prices,
            name="Tổng chi phí",
            format="${value:,.6f}",
            styles=styles,
        ),
    )
    indicators_total_2 = pn.Row(
        pn.indicators.Number(
            value=total_tokens,
            name="Số lượng token",
            format="{value:,.0f}",
            styles=styles,
        ),
        pn.indicators.Number(
            value=prices / count,
            name="Chi phí trung bình",
            format="${value:,.6f}",
            styles=styles,
        ),
    )

    feedbacks = get_feedbacks(session_input.value)
    feedback_dashboard = render_feedbacks(feedbacks)

    row_lef = pn.Row(
        indicators_total_1,
        indicators_total_2,
    )

    total_dashboard = pn.Column(
        pn.pane.Markdown("# Tổng quát"),
        pn.Row(row_lef, sizing_mode="stretch_width"),
        *feedback_dashboard
    )

    text = pn.pane.Markdown(
        "# Chi tiết"
    )
    detail_dashboards = []
    for log in logs:
        detail_dashboards.append(pn.Column(
            render_detail(log)
        ))

    dashboard_objects = [
        dashboard_refresher,
        total_dashboard,
        text,
        *detail_dashboards
    ]

    return dashboard_objects


dashboard = pn.Column()
# dashboard.objects = build_dashboard()

# TODO: session screen


def get_sessions():
    url = f"{API_URL}/sessions"
    response = requests.get(url)
    print(response)
    return response.json()


def build_sessions():
    sessions = get_sessions()
    for sess in sessions:
        sess['session_details'] = dedent(f"""<pre>
{str(sess['session_details'][0]['role']).upper()}: {sess['session_details'][0]['content']}
{str(sess['session_details'][1]['role']).upper()}: {sess['session_details'][1]['content']} 
</pre>""")
    df = pd.DataFrame(sessions)
    
    if not sessions:
        df = pd.DataFrame(columns=['session_id', 'session_details', 'latest_created_at'])

    filters = {
        'session_id': {'type': 'input', 'func': 'like', 'placeholder': 'Enter Session'},
    }
    table = pn.widgets.Tabulator(
        df[['session_id', 'session_details', 'latest_created_at']], 
        sizing_mode='stretch_width',
        layout='fit_columns',
        widths={
            'index': 80,
            "session_details": 1000
        },
        formatters={
            "session_details": HTMLTemplateFormatter()
        },
        page_size=10,
        pagination='local',
        buttons={
            'action': '<i class="fa fa-sign-in"></i>',
        },
        header_filters=filters,
        disabled=True,

    )
    
    # Nhãn Selected Session ID
    label = pn.pane.Markdown("### Selected Session ID:")

    # Ô nhập Selected Session ID (chỉ đọc)
    input_text = pn.widgets.TextInput(disabled=True)
    input_text.value = session_input.value

    # Nút Go to Session
    button = pn.widgets.Button(name="Go to session")

    
    
    # input_text = pn.widgets.TextInput(
    #     name="Selected Session ID: ", disabled=True,
    # )
    # input_text.value = session_input.value
    # button = pn.widgets.Button(
    #     name="Go to session")
    button.js_on_click(code="""
        const urlParams = new URLSearchParams(window.location.search);
        const session_id = urlParams.get('session_id');
        if (!session_id) {
            alert('Please select a session first');
            return;
        }
        var url = `?session_id=${session_id}`;
        window.location.href = url;
        """)
    
    # Tạo layout dạng lưới (2 cột, 2 hàng)
    layout = pn.Row(
        label, 
        pn.Spacer(width=10),
        input_text, 
        pn.Spacer(width=10),
        button
    )

    def update(e):
        if not e:
            return
        session_id = df.iloc[e.row]['session_id']
        session_input.value = session_id
        input_text.value = session_id
    # table.on_click(l)
    table.on_click(lambda e: update(e))
    return [layout, pn.Spacer(height=20), table]


sessions = pn.Column()
sessions.objects = build_sessions()

# Modal screen

template = pn.template.BootstrapTemplate(
    title="Chatbot tư vấn tuyển sinh đại học", 
    favicon="assets/images/favicon.png",
    header=None)


def post_feedback(data: dict):
    url = f"{API_URL}/feedbacks"
    response = requests.post(url, json=data)
    return response.json()


def build_feedback_modal(template: pn.template.BootstrapTemplate):
    template.modal.append("""
# Dưới đây là mức độ hài lòng của người dùng đối với Chatbot
---

## 1. Rất không hài lòng (Very Dissatisfied)
- Chatbot không hiểu yêu cầu của người dùng.  
- Cung cấp thông tin sai hoặc không hữu ích.  
- Tương tác khó khăn, gây khó chịu.  

## 2. Không hài lòng (Dissatisfied)
- Chatbot hiểu một phần yêu cầu nhưng xử lý không chính xác.  
- Trả lời chậm hoặc thiếu logic.  
- Không giải quyết được vấn đề người dùng mong đợi.  

## 3. Trung lập (Neutral)
- Chatbot xử lý yêu cầu ở mức cơ bản nhưng không có gì nổi bật.  
- Cung cấp thông tin đúng nhưng không linh hoạt.  
- Không gây ấn tượng mạnh mẽ, chỉ đạt mức chấp nhận được.  

## 4. Hài lòng (Satisfied)
- Chatbot hiểu rõ yêu cầu và trả lời nhanh chóng.  
- Cung cấp thông tin chính xác, có ích.  
- Giao tiếp mượt mà, tạo trải nghiệm dễ chịu cho người dùng.  

## 5. Rất hài lòng (Very Satisfied)
- Chatbot xuất sắc trong việc hiểu và xử lý yêu cầu.  
- Cung cấp câu trả lời chi tiết, hữu ích, vượt mong đợi.  
- Mang lại trải nghiệm thân thiện, linh hoạt, và đáng nhớ.  
                          """)

    template.modal.append("""# Đánh Giá""")
    radio_group = pn.widgets.RadioBoxGroup(options=[
                                           '5', '4', '3', '2', '1'], value='5', inline=True, styles={
                                               "padding": "0px 0px 0px 10px"
    })

    template.modal.append(radio_group)

    text_input = pn.widgets.TextAreaInput(
        name='', auto_grow=True, max_rows=10, rows=6, sizing_mode='stretch_width')

    template.modal.append("""# Lời nhận xét""")
    template.modal.append(text_input)

    submit_btn = pn.widgets.Button(name="Gửi")

    def handle_submit(event):
        if not session_input.value:
            pn.state.notifications.error(
                'Vui lòng hội thoại trước khi gửi phản hồi', duration=1000)
            return
        data = {
            "rating": radio_group.value,
            "content": text_input.value,
            "session_id": session_input.value
        }
        post_feedback(data)

        template.close_modal()
        radio_group.value = "5"
        text_input.value = ""
        pn.state.notifications.info(
            'Phản hồi của bạn đã được ghi nhận', duration=1000)

    submit_btn.on_click(handle_submit)

    template.modal.append(submit_btn)


build_feedback_modal(template)
modal_btn = pn.widgets.Button(name="Gửi phản hồi")


def open_feedback_modal(event):
    template.open_modal()


modal_btn.on_click(open_feedback_modal)


tab = pn.Tabs(
    ("Chatbot", pn.Column(chatPanel,
     pn.Row(q1_btn, q2_btn, q3_btn, name="Follow-up questions"),
    )),
    # ("Dashboard", dashboard),
    ("Sessions", sessions),
)
template.header.append(modal_btn)
template.main.append(tab)

template.servable()
