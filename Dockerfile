FROM python:3.11

# install dependencies firstly to use Dockers caching
COPY requirements.txt requirements.txt
pip install -r requirements.txt
# copy container folder
WORKDIR /app
COPY . /app

# RUN mkdir /mnt/knps_testumgebung && \

CMD []
