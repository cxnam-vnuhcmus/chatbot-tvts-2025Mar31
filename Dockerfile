FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY chatbot-tvts-KMS/KMSUser /app/KMSUser
COPY chatbot-tvts-KMS/KMSDashboard /app/KMSDashboard
COPY chatbot-tvts-Chatbot/ChatbotUI /app/ChatbotUI

RUN pip install --no-cache-dir -r KMSUser/requirements.txt 
RUN pip install --no-cache-dir -r KMSDashboard/requirements.txt

COPY chatbot-tvts-KMS/common /app/common
COPY chatbot-tvts-KMS/conf /app/conf
COPY chatbot-tvts-KMS/assets /app/assets
COPY chatbot-tvts-KMS/templates /app/templates


ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    BOKEH_ALLOW_WS_ORIGIN="*" \
    BOKEH_LOG_LEVEL="info"


ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

COPY chatbot-tvts-Chatbot/config.py /app/ChatbotUI/WebApp/config.py
COPY chatbot-tvts-Monitoring/MonitoringDashboardPython /app/MonitoringDashboardPython

COPY thumbnails /app/thumbnails

COPY chatbot-tvts-KMS/KMSUser/WebApp/kms_user.py /app/KMSUser/WebApp/kms_user.py
COPY chatbot-tvts-KMS/KMSDashboard/WebApp/kms_admin.py /app/KMSDashboard/WebApp/kms_admin.py
COPY chatbot-tvts-Chatbot/ChatbotUI/WebApp/app2_Chatbot_System.py /app/ChatbotUI/WebApp/app2_Chatbot_System.py
COPY chatbot-tvts-Monitoring/MonitoringDashboardPython/main.py /app/MonitoringDashboardPython/main.py

COPY index.py /app/index.py


RUN useradd -m myuser \
    && chown -R myuser:myuser /app

USER myuser

EXPOSE 6822

CMD ["panel", "serve", \
    "index.py", \
    "KMSUser/WebApp/kms_user.py", \
    "KMSDashboard/WebApp/kms_admin.py", \
    "ChatbotUI/WebApp/app2_Chatbot_System.py", \
    "MonitoringDashboardPython/main.py", \
    "--address", "0.0.0.0", \
    "--port", "6822", \
    "--cookie-secret", "KMS!@#cookie123456secretUITServerPanel", \
    "--basic-auth", "conf/accounts.json", \
    "--basic-login-template", "templates/basic_login.html", \
    "--logout-template", "templates/logout.html", \
    "--index", "index.py", \
    "--static-dirs", "assets=assets","thumbnails=thumbnails", \
    "--allow-websocket-origin", "*", \
    "--check-unused-sessions", "3600000"]


