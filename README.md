<<<<<<< HEAD
# chatbot-tvts-release-2025Mar31
=======
# chatbot-tvts-UIs

Python version: `python:3.9`

## Production Setup

```
env: .env.prod
# these variables definied below must be reconfigured

API_URL= "http://192.168.10.115:6811" # chatbot api
MONIROTING_EVALUATOR_SERVICE = "http://192.168.10.115:6821" # monotiring api
KMS_PROCESSOR_API="http://192.168.10.115:6802" # prod api
KMS_SCANNER_API="http://192.168.10.115:6803" # scanner api
```

1. start run: `make`
2. shutdown run: `make stop`
>>>>>>> 3474825 (Initial commit)
