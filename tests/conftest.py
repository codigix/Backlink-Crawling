import os

# .env may say USE_CELERY=1 (production); tests must never push work into the real Redis queue. Celery tests set it themselves.
os.environ["USE_CELERY"] = "0"
