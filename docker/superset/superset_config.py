import os


SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change-me-for-local-dev")
WTF_CSRF_ENABLED = False
