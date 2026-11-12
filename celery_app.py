"""Central Celery application. Redis is only the broker (and a tiny counter store); PostgreSQL stays the source of truth.

All settings come from the environment / .env (see .env.example). Import this module, not Celery directly.
"""

import os
from pathlib import Path

from celery import Celery
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")  # real environment variables still win

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
BROKER_URL = os.environ.get("CELERY_BROKER_URL", REDIS_URL)
RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", REDIS_URL)
TASK_TIME_LIMIT = int(os.environ.get("CELERY_TASK_TIME_LIMIT", 900))  # seconds; hard limit (also the lease length, see tasks.py)
MAX_RETRIES = int(os.environ.get("CELERY_MAX_RETRIES", 5))
RETRY_BACKOFF_MAX = int(os.environ.get("CELERY_RETRY_BACKOFF_MAX", 300))
MAX_QUEUED = int(os.environ.get("CELERY_MAX_QUEUED", 4))  # crawl tasks allowed to wait in the queue; more would only pile up

app = Celery("backlink", broker=BROKER_URL, backend=RESULT_BACKEND, include=["tasks"])
app.conf.update(
    task_acks_late=True,  # acknowledged only after the task ends: a worker that dies mid-task gets it redelivered ...
    task_reject_on_worker_lost=True,  # ... and the redelivered task re-claims from PostgreSQL, it never trusts the old message
    worker_prefetch_multiplier=1,  # one task per worker slot: a long crawl never hides queued tasks behind it
    task_time_limit=TASK_TIME_LIMIT, task_soft_time_limit=TASK_TIME_LIMIT - 60,  # not enforced by the Windows solo pool; tasks also enforce it themselves
    task_ignore_result=True, result_expires=3600,  # nothing reads task results: PostgreSQL holds them
    broker_connection_retry_on_startup=True,
    broker_transport_options={"visibility_timeout": TASK_TIME_LIMIT * 2, "socket_connect_timeout": 5, "socket_timeout": 10},
    task_publish_retry_policy={"max_retries": 2, "interval_start": 0.2, "interval_step": 0.5, "interval_max": 1},  # Redis down: fail in seconds, not minutes  # Redis re-offers unacked tasks after this; must exceed the longest task
    worker_send_task_events=True, task_send_sent_event=True,  # Flower
    task_default_queue="celery",
)
if BROKER_URL.startswith("filesystem"):  # Redis-free test broker (tests/run_celery_live.py); never used in production
    d = os.environ["LIVE_FS"]
    app.conf.broker_transport_options = {"data_folder_in": d, "data_folder_out": d, "control_folder": d + "/control"}
