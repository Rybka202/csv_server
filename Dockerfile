FROM python:3.11

RUN mkdir /csv_server

WORKDIR /csv_server

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

RUN chmod a+x *.sh