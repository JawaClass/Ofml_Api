FROM python:3.11

# container folder
WORKDIR /app
COPY . /app

RUN mkdir /mnt/knps-testumgebung && \
pip install -r requirements.txt

CMD []
