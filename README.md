# Twython to Kafka

This project implements a system for writing Twitter submissions to Kafka using [Twython](https://github.com/ryanmcgrath/twython).

## Usage

```
docker build . -t twitter

docker run -it --rm twitter ./run.py\
           --twitter-client-id <twitter_client_id>\
           --twitter-client-secret <twitter_client_secret>\
           -b <kafka_broker1:port1>[,kafka_broker2:port2] \
           -q <query>
```

You may run `docker run -it --rm python run.py --help` for more information.
