import panel as pn
import json
from pathlib import Path

def load_permissions():
    try:
        with open('conf/permissions.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}

def check_admin_access(username, permissions):
    if username not in permissions:
        return 0, f"Người dùng '{username}' không tồn tại trong hệ thống"
    
    user_perms = permissions[username]
    if not user_perms.get('approve') and not user_perms.get('input'):
        return 3, f"Người dùng '{username}' không có quyền truy cập hệ thống quản trị"

    if user_perms.get('approve'):   #admin
        return 1, ""
    
    if user_perms.get('input'):     #editor
        return 2, ""

    return 3, ""                    #user

username = pn.state.user  # Lấy username từ Basic Auth

permissions = load_permissions()
user_type, error_msg = check_admin_access(username, permissions)

print(user_type)

# Chuyển hướng dựa trên quyền
if user_type == 1:
    pn.pane.HTML("<script>window.location.href='/kms_admin';</script>").servable()
elif user_type == 2:
    pn.pane.HTML("<script>window.location.href='/kms_user';</script>").servable()
elif user_type == 3:
    pn.pane.HTML("<script>window.location.href='/app2_Chatbot_System';</script>").servable()
else:
    create_error_page(error_msg).servable()