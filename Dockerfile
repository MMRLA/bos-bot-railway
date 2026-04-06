FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --upgrade pip setuptools && \
    pip install -r requirements.txt && \
    pip install git+https://github.com/rg-3/ctrader-open-api.git

COPY . .

CMD ["python", "bos_bot_v3.py"]
