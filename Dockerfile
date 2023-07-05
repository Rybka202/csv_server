FROM python:3.11

RUN mkdir /csv_server

WORKDIR /csv_server

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

RUN mkdir ./storage_csv

RUN chmod a+x *.sh