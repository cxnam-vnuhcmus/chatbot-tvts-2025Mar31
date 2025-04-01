run: build
	cd ./chatbot-tvts-Chatbot/ && make prod_api_up && cd ..
	cd ./chatbot-tvts-KMS/ && make prod_api_up && cd ..
	cd ./chatbot-tvts-Monitoring/MonitoringEvaluator/ && make prod_api_up && cd ../.. 
	docker compose up -d --build

stop:
	cd ./chatbot-tvts-Chatbot/ && make prod_api_down && cd ..
	cd ./chatbot-tvts-KMS/ && make prod_api_down && cd ..
	cd ./chatbot-tvts-Monitoring/MonitoringEvaluator/ && make prod_api_down && cd ../.. 
	docker compose down

build:
	docker build .
