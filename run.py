#!/usr/bin/env python

import argparse
import datetime
import json
import logging
import os
import time

from kafka import KafkaProducer
from twython import Twython, TwythonRateLimitError

logging.basicConfig(format="%(asctime)s %(levelname)s: %(name)s: %(message)s",
                    level=logging.INFO)
log = logging.getLogger(os.path.basename(__file__))


class Arguments(dict):
    """ Dictionary of arguments with dot notation. """
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

def main(**args):
    """ Query tweets and stream them to Kafka. """
    args = Arguments(args) or get_args()
    max_id = args.max_id
    prev_time = time.time()

    captured = 0
    finished = False
    max_retries = 3
    results = []

    if args.kafka_brokers:
        producer = KafkaProducer(bootstrap_servers=args.kafka_brokers,
                                 api_version=(0, 10, 0),
                                 value_serializer=lambda v:
                                 json.dumps(v).encode("utf-8"))

    if args.output_json:
        j = open(args.output_json, "w")

    twitter = init_twitter_api(args.twitter_client_id,
                               args.twitter_client_secret)

    while not finished:
        previous_results = results
        retries = 0

        try:
            results = twitter.search(
                q=args.query,
                tweet_mode=args.tweet_mode,
                count=args.count,
                lang=args.lang,
                max_id=max_id,
                since_id=args.since_id,
                geocode=args.geocode,
            )["statuses"]

            if not results\
            or previous_results == results:
                finished = True
                break

            for status in results:
                max_id = status["id"] - 1

                if status["id"] and status["created_at"]:
                    if args.kafka_brokers:
                        producer.send(args.topic,
                                      process_entry(status),
                                      key=str.encode(status["id_str"]))
                    if args.output_json:
                        json.dump(status, j)
                        j.write("\n")
                    captured += 1
                else:
                    log.error(f"Found invalid document: {status}")

                dt = time.time() - prev_time
                if dt > 10:
                    log.info(f"Captured {captured} tweet(s).")
                    prev_time = time.time()

                cond1 = (args.limit and int(captured) >= int(args.limit))
                cond2 = (args.max_id and int(max_id) >= int(args.max_id))
                cond3 = (args.since_id and int(max_id) <= int(args.since_id))

                if cond1 or cond2 or cond3:
                    finished = True
                    break

        except Exception as e:
            if "429 (Too Many Requests)" in str(e):
                twitter = init_twitter_api(args.twitter_client_id,
                                           args.twitter_client_secret)

            elif any(x in str(e) for x in ["500 (Internal Server Error)",
                                           "503 (Service Unavailable)"]):
                if retries == max_retries:
                    log.warning(f"{e}. Reached maximum retries. Skipping...")
                    retries = 0
                    break

                log.warning(f"{e}. Sleeping for 60 seconds...")
                time.sleep(60)

            elif any(x in str(e) for x in ["401 (Unauthorized)",
                                           "403 (Forbidden)",
                                           "404 (Not Found)"]):
                log.warning(f"{e}. Skipping...")
                break

            else:
                raise e

    log.info(f"Total of {captured} captured tweet(s).")
    return twitter


def init_twitter_api(client_id, client_secret, endpoint="tweets"):
    """
    Returns initialized twython instance.
    Supports multiple user API credentials.
    """
    remaining = 0

    if isinstance(client_id, str):
        client_id = [client_id]

    if isinstance(client_secret, str):
        client_secret = [client_secret]

    if not client_id or not client_secret:
        raise ValueError("Missing CLIENT_ID and CLIENT_SECRET for Twitter API authentication.")

    while True:
        tts = 900

        for i in range(len(client_id)):
            try: # authenticate
                twitter = Twython(client_id[i], client_secret[i], oauth_version=2)
                access_token = twitter.obtain_access_token()
                twitter = Twython(client_id[i], access_token=access_token)
                rate_limit = twitter.get_application_rate_limit_status(resources="search")
                resource = rate_limit["resources"]["search"]["/search/tweets"]
                remaining = resource["remaining"]
                reset = resource["reset"] - int(time.time()) + 1

                if remaining:
                    log.info(f"Authenticated. {remaining} remaining requests...")
                    return twitter

                elif tts > reset:
                    tts = reset

            except TwythonRateLimitError:
                pass

            except Exception as e:
                log.warning(e)

        if not remaining:
            log.info(f"0 requests left. Sleeping for {tts} seconds...")
            time.sleep(tts)


def get_args():
    """ Read and return commandline arguments or show help text if necessary. """
    parser = argparse.ArgumentParser(description="""
        Command line interface for writing tweets to Kafka.
        """)

    parser.add_argument("-b", "--kafka_brokers",
                        help="List of direct Kafka brokers",
                        required=True)
    parser.add_argument("-t", "--topic",
                        default="ingest.twitter")

    parser.add_argument("-k", "--twitter-client-id",
                        help="user application key (required)",
                        default=os.environ.get("TWITTER_CLIENT_ID"))
    parser.add_argument("-s", "--twitter-client-secret",
                        help="user application secret (required)",
                        default=os.environ.get("TWITTER_CLIENT_SECRET"))

    parser.add_argument("-q", "--query",
                        help="to search tweets (required if --geocode not set)")
    parser.add_argument("-l", "--lang",
                        help="2-letter code identifier (e.g. 'en' for english)")
    parser.add_argument("-g", "--geocode",
                        help="coordinates as in 'lat,long,radius' (required if --query not set)")

    parser.add_argument("--count",
                        help="batch size to get from Twitter",
                        type=int,
                        default=100)
    parser.add_argument("--limit",
                        help="maximum number of tweets to capture",
                        type=int,
                        default=0)
    parser.add_argument("--max-id",
                        help="oldest tweet ID to stop search",
                        type=int,
                        default=0)
    parser.add_argument("--since-id",
                        help="newest tweet ID to stop search",
                        type=int,
                        default=0)
    parser.add_argument("--tweet-mode",
                        help="whether to return extended entities",
                        default="extended")

    parser.add_argument("--output-json",
                        help="write tweets to JSON file",
                        default=False)

    args = parser.parse_args()
    return args


def process_entry(entry):
    return {
        "id":  entry["id_str"],
        "url": "https://twitter.com/%s/status/%s" % (entry["user"]["screen_name"], entry["id_str"]),
    }


if __name__ == "__main__":
    main()
