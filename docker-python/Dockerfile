FROM python:3.10-slim

WORKDIR /var/docker-python

COPY requirements.txt /var/docker-python

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

RUN apt-get update && apt-get install git
RUN git config user.name "yamahan3"
RUN git config user.email "hanndai6@gmail.com"

