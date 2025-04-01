import panel as pn
from kms_admin_app import KMSAdmin
import json
from pathlib import Path


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



username = pn.state.user

permissions = load_permissions()

has_access, error_msg = check_admin_access(username, permissions)
    
if has_access:
    admin_ui = KMSAdmin(username)
    app = admin_ui.get_layout()
    
    app.header[-1].object = f"""
    <div style="display: flex; align-items: center; margin: 10px 20px;">
        <span style="color: #ccc; margin-right: 5px;">👤</span>
        <span style="color: #666;">{username}</span>
        <span style="color: #666; margin: 0 8px;">|</span>
        <a href="/logout" style="
            color: #666;
            font-weight: bold;
            font-size: 14px;
            text-decoration: none;
            background-color: rgba(255,255,255,0.2);
            padding: 4px 12px;
            border-radius: 4px;
        ">Đăng xuất</a>
    </div>
    """
else:
    app = create_error_page(error_msg)

app.servable()

