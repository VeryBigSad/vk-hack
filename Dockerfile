FROM python:3.11.1-slim-buster

WORKDIR /app

RUN apt-get update && apt-get install -y netcat

COPY requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY entrypoint.sh entrypoint.sh
RUN chmod +x entrypoint.sh

COPY ./app /app
RUN pybabel compile -d locales -D messages;

ENTRYPOINT ["sh", "./entrypoint.sh"]
