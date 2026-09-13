FROM mcr.microsoft.com/playwright/python:v1.45.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["gunicorn", "fastbull_app:app"]
