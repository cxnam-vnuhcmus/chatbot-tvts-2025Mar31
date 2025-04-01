# Chatbot TVTS

Maintenance: Thinh Ngo <thinhngo1198@gmail.com>

## Documentation: [Link]()

## Specifications & requirements

1. Framework/libraries: Flask, OpenAI, Panel , Docker, Docker Compose.
2. Database: PostgreSQL, ChormaDB
3. Logger: ...
4. Tools: ...

## Project structure

```
.(root folder)
├── ChatbotAgent
│ ├── __init__.py
│ ├── bot.py
│ └── v1
│ ├── __init__.py
│ └── chatbot_agent_app.py
├── ChatbotTester
│ ├── __init__.py
│ ├── bot.py
│ └── eval.ipynb
├── ChatbotUI
│ └── WebApp
│       ├── __init__.py
│       └── app2_Chatbot_System.py
├── Dockerfile.prod
├── Dockerfile.venv
├── Makefile
├── README.MD
├── config.py
├── docker-compose.dev.yml
├── docker-compose.prod.yml
├── entrypoint.sh
├── foundation.py
├── models.py
├── prompts.py
├── requirements.txt
├── utils.py
└── wsgi.py
```

## Development Setup

```
env: .env
tools: devcontainer
```

1. Python version: `python:3.10.13`
2. Setup environment: `make dev`
3. Start agent (api): `make start_agent`
4. Start app (webapp): `make start_app`

## Production Setup

```
env: .env.prod
```

1. start run: `make prod_up`
2. shutdown run: `make prod_down`

## APIS:

- Completion:
  - Path: /completion
  - Inputs:
    - stream: boolean
    - msg: string
    - session_id: str
- Conversations:
  - Path: /conversations/<session_id>
  - Inputs:
    - session_id: str
- Logs:
  - Path: /logs/<session_id>
  - Inputs:
    - session_id: str

## DEVELOPMENT NOTE:

### DONE:

- Chatbot v1
- Chatbot tester
- CHatbot ui
- Add session
- intents in json file
- List conversation by session id
- Restructure log
- api list logs
- dashboard
- followup questions
- api feedbacks

### TODO:
- Restructure webapp
- cache answers