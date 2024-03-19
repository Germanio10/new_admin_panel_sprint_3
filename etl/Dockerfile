FROM python:3.10 AS back

WORKDIR /opt/app

COPY requirements.txt requirements.txt
COPY ./entrypoint.sh /entrypoint

RUN  pip install --upgrade pip --no-cache-dir\
     && pip install -r requirements.txt --no-cache-dir

COPY . .

RUN chmod +x /entrypoint
ENTRYPOINT ["/entrypoint"]