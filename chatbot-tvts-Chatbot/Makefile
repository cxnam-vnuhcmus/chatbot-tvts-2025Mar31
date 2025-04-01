# for development only
dev: dev_build
	docker compose -f docker-compose.dev.yml up -d

dev_build:
	docker compose -f docker-compose.dev.yml build
	
dev_down:
	docker compose -f docker-compose.dev.yml down

start_app:
	panel serve ChatbotUI/WebApp/app2_Chatbot_System.py --port 8000 --allow-websocket-origin=0.0.0.0:8000 --autoreload

start_agent:
	flask --app ChatbotAgent/v1/chatbot_agent_app --debug run --host=0.0.0.0 --port=6811
	
# for production

start_app_prod:
	panel serve ChatbotUI/WebApp/app2_Chatbot_System.py --port 8000 --allow-websocket-origin="*" --cookie-secret Chatbot!@#cookie123456secretUITServerPanel --basic-auth conf/accounts.json --basic-login-template ChatbotUI/templates/basic_login.html --index ChatbotUI/templates/index.html  --static-dirs thumbnails=ChatbotUI/assets/thumbnails assets=ChatbotUI/assets

prod_up:
	docker compose -f docker-compose.prod.yml up -d --build

prod_down:
	docker compose -f docker-compose.prod.yml down

prod_api_up:
	docker compose -f docker-compose.api-prod.yml up -d --build

prod_api_down:
	docker compose -f docker-compose.api-prod.yml down
