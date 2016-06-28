# IMAGE docker.mstry.io/etl/skrape
FROM alpine:3.4
MAINTAINER Adam Haymond

WORKDIR /src

RUN apk add -U \
  bash\
  mysql-client\
  openssl\
  wget\
  && rm -rf /var/cache/apk/*

RUN wget https://github.com/MasteryConnect/docker-cron/releases/download/v1.0/docker-cron -O /usr/local/bin/docker-cron --no-check-certificate

RUN chmod +x /usr/local/bin/docker-cron

COPY skrape /usr/local/bin/
COPY run.sh ./

CMD docker-cron --seconds=0 --minutes=0 --hours=4 ./run.sh
