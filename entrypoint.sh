#!/bin/sh

while ! nc -z db 5432; do
  sleep 0.1
done

python3 manage.py collectstatic --no-input
python3 manage.py migrate
python3 manage.py loaddata movies.json

uwsgi --http :8000 --chdir /opt/app --module config.wsgi:application

exec "$@"