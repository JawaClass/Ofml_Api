FROM python:3.11

WORKDIR /app

# install dependencies firstly to use Dockers caching
COPY requirements.txt ./
RUN pip install -r requirements.txt

# copy everything
COPY . .

CMD []
