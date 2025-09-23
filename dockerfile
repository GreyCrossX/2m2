FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps (Pipenv)
COPY Pipfile Pipfile.lock /app/
RUN pip install --no-cache-dir pipenv && \
    PIPENV_YES=1 pipenv install --system --deploy --ignore-pipfile

# App code
COPY ./app /app/app
COPY ./app/services /app/services
COPY alembic.ini /app/alembic.ini
COPY alembic /app/alembic
COPY ./app/celery_app.py /app/celery_app.py

COPY ./app/scripts /app/scripts
RUN chmod +x /app/scripts/*.sh

EXPOSE 8000

# Default CMD for the web app; other services override via compose
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
