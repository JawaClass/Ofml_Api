FROM python:3.11

# container folder
WORKDIR /app
COPY . /app

RUN pip install -r requirements.txt

CMD python schedule_db_update.py
