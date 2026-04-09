# Railway: build a imagem com Python 3.11 + Playwright Chromium (deps de sistema incluídas).
# O Procfile continua `web: python api.py`; este CMD alinha com api.py (PORT do Railway).

FROM python:3.11-slim-bookworm

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt \
    && python -m playwright install-deps chromium \
    && python -m playwright install chromium

COPY . .

# Railway injeta PORT em runtime; api.py já prioriza os.environ["PORT"].
EXPOSE 8000

CMD ["python", "api.py"]
