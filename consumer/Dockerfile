FROM python:3.10.4 as app
  
WORKDIR /app

RUN groupadd -g 1000 app && useradd -u 1000 -g app -m app && \
    chown app:app /app && apt-get update && apt-get install -y --no-install-recommends && apt-get clean

ENV PATH=/home/app/.local/bin:${PATH}

COPY ["requirements.txt", "/app"]

RUN pip3 install --no-cache-dir -r requirements.txt

COPY --chown=app:app [".", "/app"]

