import argparse
import requests
import datetime
import json
import random
import time
import faker
from google.cloud import pubsub_v1

def build_uid_registration():
    """
    Generates random user ids with some overlap to simulate a real world
    user behaviour on website.
    :return: A slowly changing random number.
    """
    elapsed_tens_minutes = int((time.time() - start_time) / 600) + 1
    present_millis = int(1000 * (time.time() - int(time.time())))

    if present_millis == 0:
        present_millis = random.randint(1,1000)

    if logging_enabled:
        print('generating user_id: elapsed_tens_minute = {}, present_millis = {}'.format(
            elapsed_tens_minutes, present_millis))

    uid = random.randint(elapsed_tens_minutes + present_millis,
                        (10 + elapsed_tens_minutes) * present_millis)
    # Perform user registration on the website
    currency = fake.currency_code()
    # not supported currencies
    while currency in not_supported:
        currency = fake.currency_code()
    return uid, dict(
                    user_name = fake.name(),
                    user_phone = fake.msisdn(),
                    user_email = fake.email(),
                    debit_amount = float(0),
                    credit_amount = float(0),
                    currency = currency
                    )


def build_bet_results():
    """ Generates an event message imitation
    :return: A random event message
    """
    uid = random.choice(list(users_pool))
    results = random.randint(1,10)
    if results >= 6:
        results = 'win'
        bet_amount = round(random.uniform(10, 1000), 2)
        win_amount = round(bet_amount * random.uniform(0.5, 4), 2)
        users_pool[uid]['debit_amount'] += bet_amount
        users_pool[uid]['debit_amount'] = round(users_pool[uid]['debit_amount'], 2)
        users_pool[uid]['credit_amount'] += win_amount
        users_pool[uid]['credit_amount'] = round(users_pool[uid]['credit_amount'], 2)
    else:
        results = 'loss'
        bet_amount = round(random.uniform(10, 1000), 2)
        win_amount = float(0)
        users_pool[uid]['debit_amount'] += bet_amount
        users_pool[uid]['debit_amount'] = round(users_pool[uid]['debit_amount'], 2)

    # generate conversion rate on the moment of bet
    url = 'https://v6.exchangerate-api.com/v6/' + key + '/pair/EUR/' + users_pool[uid]['currency']
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f'{response.status_code} error happened while {url} requested.')
    data = response.json()
    print(users_pool[uid]['currency'])
    conversion = round(float(data['conversion_rate']), 2)
    return dict(
                uid=uid, # Use random uids of users currently registered on website
                user_info = users_pool[uid],
                round_results = results,
                bet_amount = bet_amount,
                win_amount = win_amount,
                timestamp=datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                conversion_rate_EUR = conversion
                )


def send_to_pub_sub(message):
    """ Sends the provided payload as JSON to Pub/Sub.
    :param message: the Event information payload
    :return: the published message.
    """
    return publisher.publish(topic_path,
                            data=json.dumps(message).encode('utf-8'))


if __name__ == '__main__':
    cmd_flags_parser = argparse.ArgumentParser(
        description='Publish messages to Pub/Sub',
        prefix_chars='-')
    cmd_flags_parser.add_argument('--event_count', type=int,
                                help='Number of log events to generate',
                                default=-1)
    cmd_flags_parser.add_argument('--topic', type=str,
                                help='Name of the Pub/Sub topic')
    cmd_flags_parser.add_argument('--project-id', type=str,
                                help='GCP Project Id running the Pub/Sub')
    cmd_flags_parser.add_argument('--enable-log', type=bool,
                                default=False,
                                help='print logs')
    cmd_flags_parser.add_argument('--sleep_time', type=int,
                                help='Sleep time > 100 ms',
                                default=5000)
    cmd_flags_parser.add_argument('--api-key', type=str,
                                help='API key for conversion https://www.exchangerate-api.com/')
    # Extract command-line arguments
    cmd_arguments = cmd_flags_parser.parse_args()

    # Define configuration
    logging_enabled = cmd_arguments.enable_log
    send_event_count = cmd_arguments.event_count  # Default send infinite messages
    pub_sub_topic = cmd_arguments.topic
    gcp_project_id = cmd_arguments.project_id
    not_supported = ['MRO', 'KPW', 'NIS', 'LTL'] # not supported currency codes
    publisher = pubsub_v1.PublisherClient()
    start_time = time.time()
    sleep_time = cmd_arguments.sleep_time
    if sleep_time < 100:
        cmd_flags_parser.error('Minimum sleeping time is 100ms')
    key = cmd_arguments.api_key
    topic_path = publisher.topic_path(gcp_project_id, pub_sub_topic)
    users_pool = {}
    fake = faker.Faker()

    message_count = 0

    print('ProjectId: {}\nPub/Sub Topic: {}'.format(gcp_project_id, topic_path))
    print('Sending events in background.')
    print('Press Ctrl+C to exit/stop.')

    # Infinite loop to keep sending messages to pub/sub
    while send_event_count == -1 or message_count < send_event_count:
        if not users_pool:
            registered = build_uid_registration()
            users_pool[registered[0]] = registered[1]
        elif random.randint(1, 4) == 3: # Perform user registration on each 4th event
            registered = build_uid_registration()
            users_pool[registered[0]] = registered[1]

        event_message = build_bet_results() # Stream bet results on casino website as event
        if (logging_enabled):
            print('Sending Message {}\n{}'.format(message_count + 1,
                                            json.dumps(event_message)))

        message_count += 1
        pub_sub_message_unique_id = send_to_pub_sub(event_message)

        if (logging_enabled):
            print(
                'pub_sub_message_id: {}'.format(pub_sub_message_unique_id.result()))

        # Random sleep time in milli-seconds.
        if (logging_enabled):
            print('Sleeping for {} ms'.format(sleep_time))
        time.sleep(sleep_time / 1000)
