import argparse
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
    return uid, dict(
                    user_name = fake.name(),
                    user_phone = fake.msisdn(),
                    user_email = fake.email(),
                    debit_amount = 0,
                    credit_amount = 0,
                    currency = fake.currency_code()
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
        users_pool[uid]['credit_amount'] += win_amount
    else:
        results = 'loss'
        bet_amount = round(random.uniform(10, 1000), 2)
        win_amount = 0
        users_pool[uid]['debit_amount'] += bet_amount

    return dict(
                uid=uid, # Use random uids of users currently on website
                user_info = users_pool[uid],
                round_results = results,
                bet_amount = bet_amount,
                win_amount = win_amount,
                timestamp=datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                )


def send_to_pub_sub(message):
    """ Sends the provided payload as JSON to Pub/Sub.
    :param message: the Event information payload
    :return: the published message future.
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
                                help='Sleep time > 1000 ms',
                                default=10000)
    # Extract command-line arguments
    cmd_arguments = cmd_flags_parser.parse_args()

    # Define configuration
    logging_enabled = cmd_arguments.enable_log
    send_event_count = cmd_arguments.event_count  # Default send infinite messages
    pub_sub_topic = cmd_arguments.topic
    gcp_project_id = cmd_arguments.project_id
    publisher = pubsub_v1.PublisherClient()
    start_time = time.time()
    sleep_time = cmd_arguments.sleep_time
    if sleep_time < 1000:
        cmd_flags_parser.error('Minimum sleeping time is 1000ms')
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
