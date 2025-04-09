import panel as pn
from model.conversationDto import ConversationDto
from model.recordDto import RecordDto
from model.rateDto import RateDto
from service.conversationService import get_conversation_by_id, get_all_conversations, get_total_count_conversation
from bokeh.models import Div
from bokeh.io import show
from bokeh.layouts import column
from bokeh.models.widgets import Button
from bokeh.models.widgets.tables import NumberFormatter, DateFormatter
from panel.widgets import IntSlider
import pandas as pd
import math
import plotly.graph_objects as go
pn.extension('tabulator')
pn.extension('plotly')
pn.extension(raw_css=[
    """
        .pn-wrapper .bk-panel-models-layout-Column:first-child
        {
            height: calc(100vh - 128px);
        }
    """
])

# Define a function to create a table with buttons
def create_conversation_table():
    # Create a custom function to update the table based on the page
    def update_table(page):
        nonlocal conversations
        nonlocal current_page, page_number, total_count_conversation
        nonlocal rows, df, table
        nonlocal loading
        
        loading.visible = True  # Show the loading spinner

        # update current page
        current_page = page
        page_number.object = f"Page: {current_page}/{total_page}"
        
        # Get data
        try:
            conversations = get_all_conversations(page_index=page, page_size=page_size)
        except Exception as e:
            conversations = []

        # Loop through conversations and add them to table_data
        rows = []
        for conversation in conversations:
            row = [conversation.id, conversation.first_input, conversation.avg_csat]
            rows.append(row)
        
        df = pd.DataFrame(rows, columns=column_names)
        table.value = df
        loading.visible = False  # Hide the loading spinner

    def get_total_count():
        try:
            return get_total_count_conversation()
        except Exception as e:
            return 0
    
    # Define the button actions
    def on_previous_button_click(event):
        if current_page > 1:
            update_table(current_page - 1)

    def on_next_button_click(event):
        if current_page < total_page:
            update_table(current_page + 1)

    def on_first_button_click(event):
        update_table(1)

    def on_last_button_click(event):
        update_table(total_page)

    conversations = []
    total_count_conversation = get_total_count()

    page_size = 100
    total_page = math.ceil(total_count_conversation / page_size)
    column_names = ["Id", "Content", "Avg. CSAT"]
    rows = []
    current_page = 1
    df = pd.DataFrame(rows, columns=column_names)
    
    # Page number
    page_number = pn.pane.Markdown("")
    
    # Create custom navigation buttons
    first_button = Button(label="First", width=100)
    prev_button = Button(label="Previous", width=100)
    next_button = Button(label="Next", width=100)
    last_button = Button(label="Last", width=100)

    # Attach events to buttons
    prev_button.on_click(on_previous_button_click)
    next_button.on_click(on_next_button_click)
    first_button.on_click(on_first_button_click)
    last_button.on_click(on_last_button_click)

    # Formaters
    bokeh_formatters = {
        'Avg. CSAT': NumberFormatter(format='0.0000'),
        # 'Created At': DateFormatter(format='%d-%m-%Y %H:%M:%S'),
    }

    # Create a Panel widget with the DataFrame
    widths={'Id': '25%', 'Content': '55%', 'Avg. CSAT': '20%'}
    table = pn.widgets.Tabulator(
        df,
        widths=widths, 
        sizing_mode="stretch_width", 
        disabled=True, 
        formatters=bokeh_formatters,
        styles={'max-height': '100vh', 'overflow-y': 'auto'},
    )
    
    table.on_click(show_conversation)

    # Create a loading widget
    loading = pn.pane.Markdown("Loading...")

    # get data in first page
    update_table(current_page)

    # Layout the table with pagination controls
    controls = pn.Row(
        first_button, prev_button, page_number, next_button, last_button, loading,
        styles={'align-items': 'center'}
    )
    layout = pn.Column(table, styles={'max-height': '80vh', 'overflow-y': 'auto'})
    return layout, controls

def show_conversation(event):
    if hasattr(event.model, 'source'):
        data_id = event.model.source.data['Id'][event.row]
        show_popup(data_id)

def create_conversation_details_popup():
    # Nội dung popup
    popup_content = pn.Column()

    # Popup Modal để hiển thị chi tiết hội thoại
    popup = pn.Card(
        popup_content,
        collapsible=False,
        visible=False,  
        styles={
            'position': 'fixed', 'left': '50%', 'top': '50%', 
            'transform': 'translate(-50%, -50%)', 'z-index': '1000',
            'background': 'white', 'padding': '20px',
            'width': '80%',  
            'height': '80%', 'overflow-y': 'auto', 'overflow-x': 'hidden',
            'border-radius': '8px', 'box-shadow': '0px 4px 10px rgba(0, 0, 0, 0.1)'
        }
    )

    return popup

# Handle show popup
def show_popup(conversation_id: str):
    # Show popup
    conversation_details_popup.visible = True
    conversation_details_popup.title = f"Loading conversation: {conversation_id} ..."
    conversation_details_popup[0].objects = [pn.Row()]

    # Fetch conversation detail
    conversation = get_conversation_by_id(conversation_id)
    conversation_details_popup.title = f"Conversation: {conversation_id}"

    # Change content of conversation_details_popup
    popup_content = get_popup_content(conversation.records)
    conversation_details_popup[0].objects = popup_content

    return

def get_rating_ui(record):
    ratings = pn.Row(sizing_mode='stretch_width')
    csat = pn.Row(sizing_mode='stretch_width')

    if record["is_rated"] == True:
        csat = pn.Row(
            pn.pane.Markdown(f"CSAT: {record.get('rate', {}).get('csat', 'N/A')}", styles={'color': 'green', 'font-size': '14px'}),
            sizing_mode='stretch_width'
        )
        ratings=pn.Row(
            pn.pane.Markdown(f"**Answer Relevance:** {record.get('rate', {}).get('answer_relevance', 'N/A')}", styles={'color': 'gray', 'font-size': '12px'}),
            pn.pane.Markdown(f"**Context Relevance:** {record.get('rate', {}).get('context_relevance', 'N/A')}", styles={'color': 'gray', 'font-size': '12px'}),
            pn.pane.Markdown(f"**Groundedness:** {record.get('rate', {}).get('groundedness', 'N/A')}", styles={'color': 'gray', 'font-size': '12px'}),
            pn.pane.Markdown(f"**Sentiment:** {record.get('rate', {}).get('sentiment', 'N/A')}", styles={'color': 'gray', 'font-size': '12px'}),
            sizing_mode='stretch_width'
        )
    else:
        csat = pn.Row(
            pn.pane.Markdown(f"Not rated", styles={'color': 'red', 'font-size': '14px'}),
            sizing_mode='stretch_width'
        )
        ratings=pn.Row(sizing_mode='stretch_width')

    return [ratings, csat]

# Get popup content
def get_popup_content(records: list[RecordDto]):
    record_panes = []

    for record in records:
        [ratings, csat] = get_rating_ui(record)
        
        record_pane = pn.Column(
            pn.Row(
                pn.pane.Markdown(f"**Record:** {record.get('id', '')}", styles={'color': 'gray', 'font-size': '14px'}),
                csat,
                pn.pane.Markdown(f"{record['start_time']}", styles={'color': 'gray', 'font-size': '12px', 'margin-left': "auto", }),
                sizing_mode='stretch_width'
            ),
            pn.Row(
                pn.widgets.ButtonIcon(icon="user", size="24px", width=24, height=24),
                pn.Column(
                    pn.pane.Markdown(f"{record['main_input']}", styles={'background-color': '#f3f4f6', 'padding': '10px', 'border-radius': '8px', 'width': '100%'}),
                    sizing_mode="stretch_width",
                ),
                sizing_mode='stretch_width'
            ),
            pn.Row(
                pn.widgets.ButtonIcon(icon="robot-face", size="24px", width=24, height=24),
                pn.Column(
                    pn.pane.Markdown(f"{record['main_output']}", styles={'background-color': '#fef3c7', 'padding': '10px', 'border-radius': '8px', 'width': '100%'}),
                    ratings,
                    sizing_mode="stretch_width",
                ),
                sizing_mode='stretch_width'
            ),
            pn.layout.HSpacer(height=10)
        )
        record_panes.append(record_pane)

    # Nút đóng popup
    close_button = pn.widgets.Button(name="OK", button_type="primary")
    close_button.on_click(lambda event: setattr(conversation_details_popup, 'visible', False))

    return [*record_panes, pn.Row(pn.layout.HSpacer(), close_button), pn.layout.HSpacer(height=20)]


def load_permissions():
    """
    Load permissions from a JSON file.

    Returns:
        dict: A dictionary representing the permissions loaded from the JSON file.
            If the file cannot be loaded or an error occurs, an empty dictionary is returned.
    """
    try:
        with open('conf/permissions.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        return {}

def create_error_page(message):
    """
    Create an error page using the existing login template.

    This function generates an HTML error page, using a predefined login template
    stored at `templates/basic_login.html`. If the template file exists, the error
    message is inserted into the template, and the page is returned as a Pane. 
    If the template is not found, a fallback error page is created directly in HTML.

    Args:
        message (str): The error message to be displayed on the page.

    Returns:
        pn.pane.HTML: An HTML pane containing the error page.
    """
    template_path = Path('templates/basic_login.html')
    if template_path.exists():
        with open(template_path, 'r', encoding='utf-8') as f:
            html = f.read()
            html = html.replace('{{errormessage}}', message)
            return pn.pane.HTML(html)
    else:
        return pn.pane.HTML(f"""
            <div class="wrap">
                <div class="login-form">
                    <div class="form-header">
                        <h3><img id="logo" src="assets/images/dhqg-logo.png" width="300"></h3>
                        <br>
                        <p>{message}</p>
                    </div>
                    <div class="form-group">
                        <a href="/logout" class="form-button">Đăng xuất</a>
                    </div>
                </div>
            </div>
        """)

def check_admin_access(username, permissions):
    """
    Check if a user has admin access rights.

    Args:
        username (str): The username of the user to check.
        permissions (dict): A dictionary containing user permissions, where keys are
                            usernames and values are permission data.

    Returns:
        tuple: A tuple (bool, str) where:
            - bool: `True` if the user has admin access, `False` otherwise.
            - str: An error message if the user doesn't have access, or an empty string if the user has access.
    """
    if username not in permissions:
        return False, f"Người dùng '{username}' không tồn tại trong hệ thống"
    
    user_perms = permissions[username]
    if not user_perms.get('approve'):
        return False, f"Người dùng '{username}' không có quyền truy cập hệ thống quản trị"
    
    return True, ""


# Create chatbot evalution table
def create_chatbot_evaluation_table(inputPath: str):
    # Read data file from excel
    df = pd.read_excel(inputPath)
    
    needed_columns = ['id', 'main_input', 'main_output', 'chatbot_answer', 'answer_relevance', 'groundedness', ]
    df = df[needed_columns]
    
    column_titles = {
        'main_input': 'Câu hỏi',
        'main_output': 'Câu trả lời đã kiểm định',
        'chatbot_answer': 'Chatbot trả lời',
        'answer_relevance': "Mức độ liên quan",
        'groundedness': "Mức độ chính xác",
    }

    column_widths={
        'index': '5%', 
        'id': '0%', 
        'main_input': '25%', 
        'main_output': '25%',
        'chatbot_answer': '25%',
        'answer_relevance': '10%',
        'groundedness': '10%',
    }
    
    column_configs = [
        {
            'field': col,
            'title': column_titles.get(col, col),
            'width': column_widths.get(col),
            "editable": False,
            'editor': False 
        }
        for col in df.columns.tolist()
    ]
    
    # Define column widths and formatters if needed
    bokeh_formatters = {}  # Define formatters if necessary, e.g., for dates

    # Create the Tabulator widget to display the DataFrame
    table = pn.widgets.Tabulator(df,
        widths=column_widths,
        sizing_mode="stretch_width", 
        disabled=True,  # Set to False if you want the table to be editable
        formatters=bokeh_formatters,
        styles={'max-height': '100vh', 'overflow-y': 'auto'},
        page_size=20,
        pagination='local',
        configuration={
                    'layout': 'fitColumns',  
                    'columns': column_configs
                }
    )
    
    table.hidden_columns = ['id']
    
    table.on_click(show_evaluation)
    
    return table

def create_evaluation_details_popup():
    # Nội dung popup
    popup_content = pn.Column()

    # Popup Modal để hiển thị chi tiết hội thoại
    popup = pn.Card(
        popup_content,
        collapsible=False,
        visible=False,  
        styles={
            'position': 'fixed', 'left': '50%', 'top': '50%', 
            'transform': 'translate(-50%, -50%)', 'z-index': '1000',
            'background': 'white', 'padding': '20px',
            'width': '80%',  
            'height': '80%', 'overflow-y': 'auto', 'overflow-x': 'hidden',
            'border-radius': '8px', 'box-shadow': '0px 4px 10px rgba(0, 0, 0, 0.1)'
        }
    )

    return popup

def show_evaluation(event):
    if hasattr(event.model, 'source'):
        data_source = event.model.source.data
        row_index = event.row 
        row_data = {col: data_source[col][row_index] for col in data_source}
        show_evaluation_popup(row_data)

def show_evaluation_popup(row_data: dict):
    # Show popup
    evaluation_details_popup.visible = True
    evaluation_details_popup.title = f"Testcase Id: {row_data['id']}"
    evaluation_details_popup[0].objects = [pn.Row()]
    popup_content = get_evaluation_popup_content(row_data)
    evaluation_details_popup[0].objects = popup_content

    return

def get_evaluation_rating_ui(row_data):
    ratings = pn.Row(sizing_mode='stretch_width')
    ans_rel = row_data["answer_relevance"]
    groundedness = row_data["groundedness"]
    
    ratings=pn.Row(
        pn.pane.Markdown(f"**Mức độ liên quan:** {ans_rel}", styles={'color': 'gray', 'font-size': '12px'}),
        pn.pane.Markdown(f"**Mức độ chính xác:** {groundedness}", styles={'color': 'gray', 'font-size': '12px'}),
        sizing_mode='stretch_width'
    )
    
    return ratings

def get_evaluation_popup_content(row_data: dict):
    record_panes = []

    ratings = get_evaluation_rating_ui(row_data)
    
    record_pane = pn.Column(
        pn.Row(
            pn.pane.Markdown(f" ", styles={'color': 'gray', 'font-size': '14px'}),
            sizing_mode='stretch_width'
        ),
        pn.Row(
            pn.widgets.ButtonIcon(icon="user", size="24px", width=24, height=24),
            pn.Column(
                pn.pane.Markdown(f"{row_data['main_input']}", styles={'background-color': '#f3f4f6', 'padding': '10px', 'border-radius': '8px', 'width': '100%'}),
                sizing_mode="stretch_width",
            ),
            sizing_mode='stretch_width'
        ),
        pn.Row(
            pn.widgets.ButtonIcon(icon="eye", size="24px", width=24, height=24),
            pn.Column(
                pn.pane.Markdown(f"{row_data['main_output']}", styles={'background-color': '#d1fae5', 'padding': '10px', 'border-radius': '8px', 'width': '100%'}),
                sizing_mode="stretch_width",
            ),
            sizing_mode='stretch_width'
        ),
        pn.Row(pn.Spacer(height=20)),
        pn.Row(
            pn.widgets.ButtonIcon(icon="robot-face", size="24px", width=24, height=24),
            pn.Column(
                pn.pane.Markdown(f"{row_data['chatbot_answer']}", styles={'background-color': '#fef3c7', 'padding': '10px', 'border-radius': '8px', 'width': '100%'}),
                ratings,
                sizing_mode="stretch_width",
            ),
            sizing_mode='stretch_width'
        ),
        pn.layout.HSpacer(height=10)
    )
    record_panes.append(record_pane)

    # Nút đóng popup
    close_button = pn.widgets.Button(name="OK", button_type="primary")
    close_button.on_click(lambda event: setattr(evaluation_details_popup, 'visible', False))

    return [*record_panes, pn.Row(pn.layout.HSpacer(), close_button), pn.layout.HSpacer(height=20)]


# Create chatbot evalution pie charts
def create_chatbot_evaluation_pie_charts(inputPath: str):
    # Read data file from excel
    df = pd.read_excel(inputPath)

    # Average Answer Relevance pie chart
    avg_answer_relevance = df["answer_relevance"].mean()
    avg_answer_relevance_percent = avg_answer_relevance * 100 / 5
    labels_1 = ['Liên quan', 'Chưa liên quan']
    sizes_1 = [avg_answer_relevance_percent, 100-avg_answer_relevance_percent]

    fig = go.Figure(data=[go.Pie(labels=labels_1, values=sizes_1)])
    fig.update_layout(
        title={ 'text': 'Câu trả lời có liên quan đến câu hỏi', 'x': 0.5, 'xanchor': 'center' },
        height=300
    )
    pie_avg_answer_relevance = pn.pane.Plotly(fig)

    # Average Groundedness pie chart
    avg_groundedness = df["groundedness"].mean()
    avg_groundedness_percent = avg_groundedness * 100 / 5
    labels_2 = ['Đúng', 'Chưa đúng']
    sizes_2 = [avg_groundedness_percent, 100-avg_groundedness_percent]

    fig2 = go.Figure(data=[go.Pie(labels=labels_2, values=sizes_2)])
    fig2.update_layout(
        title={ 'text': 'Câu trả lời chứa thông tin đúng', 'x': 0.5, 'xanchor': 'center' },
        height=300
    )
    pie_groundedness = pn.pane.Plotly(fig2)

    return pn.Column(
        pie_avg_answer_relevance,
        pie_groundedness,
        styles={
            'width': '30%', 
        }
    )
    
###########
def create_conversation_table_from_file(inputPath: str):
    global global_df
    # Read data file from excel
    df = pd.read_excel(inputPath)
    
    needed_columns = ['id', 'main_input', 'main_output', 'answer_relevance', 'groundedness', 'context_relevance', "sentiment", "csat", "conversation_id"]
    global_df = df[needed_columns]
    
    grouped = global_df.groupby('conversation_id')
    
    summary_df = grouped.agg({
        'main_input': 'first',    # Lấy main_input đầu tiên
        'csat': 'mean'            # Tính trung bình csat
    }).reset_index()
    
    summary_df['csat'] = summary_df['csat'].round(2)

    column_titles = {
        'main_input': 'Nội dung',
        'csat': "Avg. CSAT"
    }

    column_widths={
        'index': '10%', 
        'id': '0%', 
        'main_input': '80%', 
        'main_output': '0%',
        'answer_relevance': '0%',
        'groundedness': '0%',
        'context_relevance': '0%',
        'sentiment': '0%',
        'csat': '10%',
        'conversation_id': '0%'
    }
    
    column_configs = [
        {
            'field': col,
            'title': column_titles.get(col, col),
            'width': column_widths.get(col),
            "editable": False,
            'editor': False 
        }
        for col in global_df.columns.tolist()
    ]
    
    # Define column widths and formatters if needed
    bokeh_formatters = {}  # Define formatters if necessary, e.g., for dates

    # Create the Tabulator widget to display the DataFrame
    table = pn.widgets.Tabulator(
        summary_df,
        widths=column_widths,
        sizing_mode="stretch_width", 
        disabled=True,  # Set to False if you want the table to be editable
        formatters=bokeh_formatters,
        page_size=20,
        pagination='local',
        styles={'max-height': '100vh', 'overflow-y': 'auto'},
        configuration={
                    'layout': 'fitColumns',  
                    'columns': column_configs
                }
    )
    
    table.hidden_columns = ['id', 'main_output', 'answer_relevance', 'groundedness', 'context_relevance', 'sentiment', 'conversation_id']
    
    table.on_click(show_conversation_from_file)
    
    return table

def create_conversation_details_popup_from_file():
    # Nội dung popup
    popup_content = pn.Column()

    # Popup Modal để hiển thị chi tiết hội thoại
    popup = pn.Card(
        popup_content,
        collapsible=False,
        visible=False,  
        styles={
            'position': 'fixed', 'left': '50%', 'top': '50%', 
            'transform': 'translate(-50%, -50%)', 'z-index': '1000',
            'background': 'white', 'padding': '20px',
            'width': '80%',  
            'height': '80%', 'overflow-y': 'auto', 'overflow-x': 'hidden',
            'border-radius': '8px', 'box-shadow': '0px 4px 10px rgba(0, 0, 0, 0.1)'
        }
    )

    return popup

def show_conversation_from_file(event):
    global global_df
    
    if hasattr(event.model, 'source'):
        data_source = event.model.source.data
        row_index = event.row 
        selected_conversation_id = data_source['conversation_id'][row_index]
        matching_rows = global_df[global_df['conversation_id'] == selected_conversation_id]
        show_conversation_popup_from_file(selected_conversation_id, matching_rows)
        
        # row_data = {col: data_source[col][row_index] for col in data_source}
        # show_conversation_popup_from_file(row_data)

def show_conversation_popup_from_file(conversation_id, matching_rows: list):
    # Show popup
    conversation_details_popup.visible = True
    conversation_details_popup.title = f"Conversation id: {conversation_id}"
    conversation_details_popup[0].objects = [pn.Row()]
    
    popup_content = get_conversation_popup_content_from_file(matching_rows)
    conversation_details_popup[0].objects = popup_content

    return

def get_conversation_rating_ui_from_file(row_data):    
    ratings = pn.Row(sizing_mode='stretch_width')
    csat = pn.Row(sizing_mode='stretch_width')
    
    ans_rel = row_data["answer_relevance"]
    groundedness = row_data["groundedness"]
    con_rel = row_data["context_relevance"]
    sentiment = row_data["sentiment"]
    csat_score = row_data["csat"]
    
    csat = pn.Row(
        pn.pane.Markdown(f"CSAT: {csat_score}", styles={'color': 'green', 'font-size': '14px'}),
        sizing_mode='stretch_width'
    )
    ratings=pn.Row(
        pn.pane.Markdown(f"**Answer Relevance:** {ans_rel}", styles={'color': 'gray', 'font-size': '12px'}),
        pn.pane.Markdown(f"**Context Relevance:** {con_rel}", styles={'color': 'gray', 'font-size': '12px'}),
        pn.pane.Markdown(f"**Groundedness:** {groundedness}", styles={'color': 'gray', 'font-size': '12px'}),
        pn.pane.Markdown(f"**Sentiment:** {sentiment}", styles={'color': 'gray', 'font-size': '12px'}),
        sizing_mode='stretch_width'
    )
    
    return [ratings, csat]

def get_conversation_popup_content_from_file(matching_rows: list):
    record_panes = []

    for _, row_data in matching_rows.iterrows():
        
        [ratings, csat] = get_conversation_rating_ui_from_file(row_data)
    
        record_pane = pn.Column(
            pn.Row(
                pn.pane.Markdown(f"**Record:** {row_data['id']}", styles={'color': 'gray', 'font-size': '14px'}),
                csat,
                sizing_mode='stretch_width'
            ),
            pn.Row(
                pn.widgets.ButtonIcon(icon="user", size="24px", width=24, height=24),
                pn.Column(
                    pn.pane.Markdown(f"{row_data['main_input']}", styles={'background-color': '#f3f4f6', 'padding': '10px', 'border-radius': '8px', 'width': '100%'}),
                    sizing_mode="stretch_width",
                ),
                sizing_mode='stretch_width'
            ),
            pn.Row(
                pn.widgets.ButtonIcon(icon="robot-face", size="24px", width=24, height=24),
                pn.Column(
                    pn.pane.Markdown(f"{row_data['main_output']}", styles={'background-color': '#fef3c7', 'padding': '10px', 'border-radius': '8px', 'width': '100%'}),
                    ratings,
                    sizing_mode="stretch_width",
                ),
                sizing_mode='stretch_width'
            ),
            pn.layout.HSpacer(height=10)
        )
        record_panes.append(record_pane)

    # Nút đóng popup
    close_button = pn.widgets.Button(name="OK", button_type="primary")
    close_button.on_click(lambda event: setattr(conversation_details_popup, 'visible', False))

    return [*record_panes, pn.Row(pn.layout.HSpacer(), close_button), pn.layout.HSpacer(height=20)]


# -----Dashboard UI

username = pn.state.user

permissions = load_permissions()

has_access, error_msg = check_admin_access(username, permissions)

pn.state.admin_permissions = has_access

if True:
    # # Create the table with conversations
    # conversation_table, control = create_conversation_table()

    # # Popup conversation details
    # conversation_details_popup = create_conversation_details_popup()
    
    # # Create the table conversation
    # conversation_evaluation = pn.Column(
    #     conversation_table,
    #     conversation_details_popup  # Popup hiển thị đè lên table
    # )
    
    ####
    import os
    file_path = os.path.join(os.path.dirname(__file__), "conversation-data.xlsx")
    conversation_table = create_conversation_table_from_file(file_path)
    conversation_details_popup = create_conversation_details_popup_from_file()
    conversation_evaluation = pn.Column(
        conversation_table,
        conversation_details_popup  # Popup hiển thị đè lên table
    )
    ####


    # Create table chatbot evaluation
    import os
    file_path = os.path.join(os.path.dirname(__file__), "test-data.xlsx")
    chatbot_evaluation_table = create_chatbot_evaluation_table(file_path)

    # Create chatbot evaluation pie chart
    chatbot_evaluation_pie_charts = create_chatbot_evaluation_pie_charts(file_path)

    evaluation_details_popup = create_evaluation_details_popup()
    
    evaluate_button = pn.widgets.Button(name="Thực hiện đánh giá", button_type="primary", width=200)
    
    # Overlay nền mờ
    overlay_background = pn.pane.HTML(
        """<div style="position: fixed; top: 0; left: 0; 
            width: 100vw; height: 100vh; 
            background-color: rgba(0, 0, 0, 0.4); z-index: 999;">&nbsp;</div>""",
        visible=False,
        sizing_mode="stretch_both"
    )
    
    # Hộp thoại xác nhận (giả lập)
    confirm_text = pn.pane.Markdown("**Bạn có muốn thực hiện đánh giá lại hệ thống chatbot trên toàn bộ dữ liệu kiểm thử không?**")
    confirm_yes = pn.widgets.Button(name="Đồng ý", button_type="success", width=100)
    confirm_no = pn.widgets.Button(name="Hủy", button_type="danger", width=100)
    confirm_dialog = pn.Column(
        confirm_text, 
        pn.Row(confirm_yes, confirm_no, align="center"),
        align="center", 
        visible=False, 
        styles={
            "position": "fixed",
            "top": "50%",
            "left": "50%",
            "transform": "translate(-50%, -50%)",
            "z_index": "1000",
            "box-shadow": "0 4px 20px rgba(0,0,0,0.2)",
            "padding": "20px",
            "border-radius": "8px",
            "background": "#ffffff",
            "text-align": "center"
        },
        width=500)

    
    # Popup loading (giả lập)
    loading_spinner = pn.indicators.LoadingSpinner(value=True, width=25, height=25)
    loading_text = pn.pane.Markdown("**Đang đánh giá...**")
    stop_button = pn.widgets.Button(name="Dừng", button_type="danger", width=100)
    loading_popup = pn.Column(
        pn.Row(loading_spinner, loading_text, align="center"), 
        pn.Row(stop_button, align="center"),
        align="center",
        visible=False, 
        styles={
            "position": "fixed",
            "top": "50%",
            "left": "50%",
            "transform": "translate(-50%, -50%)",
            "z_index": "1000",
            "box-shadow": "0 4px 20px rgba(0,0,0,0.2)",
            "padding": "20px",
            "border-radius": "8px",
            "background": "#ffffff",
            "text-align": "center"
        },
        width=300)

    
    def on_evaluate_click(event):
        overlay_background.visible = True
        confirm_dialog.visible = True

    def on_confirm_yes(event):
        confirm_dialog.visible = False
        loading_popup.visible = True

    def on_confirm_no(event):
        overlay_background.visible = False
        confirm_dialog.visible = False

    def on_stop_click(event):
        loading_popup.visible = False
        overlay_background.visible = False

    # Gắn callback
    evaluate_button.on_click(on_evaluate_click)
    confirm_yes.on_click(on_confirm_yes)
    confirm_no.on_click(on_confirm_no)
    stop_button.on_click(on_stop_click)

    # Create the table chatbot evaluation
    chatbot_evaluation = pn.Column(
        pn.Row(
            pn.Spacer(width=0, sizing_mode="stretch_width"),
            evaluate_button,
            sizing_mode="stretch_width"
        ),
        pn.Row(
            chatbot_evaluation_table,
            chatbot_evaluation_pie_charts,
            evaluation_details_popup
        ),
        overlay_background,
        confirm_dialog,
        loading_popup
    )

    # Create tabs
    tabs = pn.Tabs(
        ("Đánh giá Conversation", conversation_evaluation),
        ("Đánh giá ChatBot", chatbot_evaluation),
    )

    # Create the Panel layout
    dashboard = pn.Column(
        conversation_table,
        # control,
        conversation_details_popup  
    )

    
    current_page = pn.state.location.pathname

    menu_items = [
        ("Quản lý tri thức", "/kms_admin"),
        ("Đánh giá hệ thống", "/main")
    ]

    custom_css = """
    <style>
        .menu-bar button {
            font-size: 18px;
            transition: transform 0.2s ease-in-out, background-color 0.2s;
        }
        .menu-bar button:hover {
            transform: scale(1.1);
            background-color: #005f99 !important;
            color: white !important;
        }
        .menu-bar .bk-btn-primary {
            background-color: #007acc !important;
            color: white !important;
        }
        .bk-root .title {
            font-size: 20px !important;
            font-weight: bold;
        }
        .logout {
            text-align: right;
            font-size: 16px;
            font-weight: bold;
        }
    </style>
    """

    def create_menu():
        menu_buttons = []
        for name, link in menu_items:
            button_type = "primary" if current_page == link else "default"
            button = pn.widgets.Button(name=name, button_type=button_type, width=140, height=40)
            button.js_on_click(args={"url": link}, code="window.location.href = url;")
            menu_buttons.append(button)
        return pn.Row(*menu_buttons, css_classes=["menu-bar"])

    menu = create_menu()

    header_row = pn.Row(
        pn.layout.HSpacer(),
        pn.pane.HTML(
            f"""<div style="margin-top: 10px; color: #4A5568; background: #F7FAFC; padding: 10px; font-size: 16px; border-radius: 4px;">
                👤 {username} | <a href="/logout" style="color: #000000; font-weight: bold; text-decoration: none;">Đăng xuất</a>
            </div>""",
        ),
        sizing_mode='stretch_width',
        css_classes=['header']
    )

    app = pn.template.FastListTemplate(
        header=[
            pn.pane.HTML(custom_css),
            pn.Row(pn.Spacer(width=50), menu, align="start"),
            header_row
        ],
        title="M&E - HỆ THỐNG ĐÁNH GIÁ CHATBOT",
        favicon="assets/images/favicon.png",
        main=[tabs],
        theme_toggle=False
    )

else:
    app = create_error_page(error_msg)

app.servable()
