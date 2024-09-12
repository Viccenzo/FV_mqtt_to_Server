FROM python:3.10.14

WORKDIR /app

COPY src/ .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "main.py"]