import panel as pn
from kms_user_app import KMSUser
import json
from pathlib import Path


def load_permissions():
    try:
        with open('conf/permissions.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        return {}


def create_error_page(message):
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

def check_user_access(username, permissions):
    if username not in permissions:
        return False, "Người dùng không tồn tại trong hệ thống"
    
    user_perms = permissions[username]
    if not user_perms.get('input'):
        return False, f"Người dùng '{username}' không có quyền truy cập hệ thống nhập liệu"
        
    return True, ""


username = pn.state.user
permissions = load_permissions()
has_access, error_msg = check_user_access(username, permissions)

if has_access:
    user_ui = KMSUser(username)
    app = user_ui.get_layout()
    
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