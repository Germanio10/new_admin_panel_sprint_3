FROM python:3.10

WORKDIR /opt/app/

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV UWSGI_PROCESSES 1
ENV UWSGI_THREADS 16
ENV UWSGI_HARAKIRI 240
ENV DJANGO_SETTINGS_MODULE 'config.settings'

COPY requirements.txt requirements.txt

RUN apt-get update && apt-get install -y netcat-traditional \
    && mkdir -p /var/www/static/ \
    && mkdir -p /var/www/media/ \
    && mkdir -p /opt/app/static/ \
    && mkdir -p /opt/app/media/ \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY ./app .

COPY entrypoint.sh entrypoint.sh
EXPOSE 8000
CMD ["gunicorn", "-b", "0.0.0.0:8000", "config.wsgi:application"]
RUN chmod +x ./entrypoint.sh
ENTRYPOINT ["/bin/bash", "./entrypoint.sh"]
