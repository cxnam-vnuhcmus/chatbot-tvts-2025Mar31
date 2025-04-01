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
pn.extension('tabulator')

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


# -----Dashboard UI

username = pn.state.user

permissions = load_permissions()

has_access, error_msg = check_admin_access(username, permissions)

pn.state.admin_permissions = has_access

if True:
    # Create the table with conversations
    conversation_table, control = create_conversation_table()

    # Popup conversation details
    conversation_details_popup = create_conversation_details_popup()

    # Create the Panel layout
    dashboard = pn.Column(
        conversation_table,
        control,
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
        main=[dashboard],
        theme_toggle=False
    )

else:
    app = create_error_page(error_msg)

app.servable()
