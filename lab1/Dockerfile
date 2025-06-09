FROM python:3.9-slim

WORKDIR /app

COPY const.py .

COPY lecture_session.py .

COPY session_type_search.py .

COPY app.py .

COPY lab.py .

COPY requirements.txt .

RUN pip install -r requirements.txt

CMD ["python", "app.py"]
