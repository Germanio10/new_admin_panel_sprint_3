FROM python:3.10

WORKDIR /app

RUN apt-get update && apt-get install -y ncat

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip && pip install -r requirements.txt --no-cache-dir

COPY . .

RUN chmod +x entrypoint.sh

ENTRYPOINT ["/bin/bash", "./entrypoint.sh"]
