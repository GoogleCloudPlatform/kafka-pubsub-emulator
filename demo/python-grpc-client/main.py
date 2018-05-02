"""
Copyright 2018 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import argparse

import time

from google.cloud import pubsub_v1


def publish_message(project, topic, data):
  publisher = pubsub_v1.PublisherClient()
  topic_path = publisher.topic_path(project, topic)
  publisher.publish(topic_path, data=data.encode('utf-8'))
  print('Published message.')


def subscribe_messages(project, subscription):
  subscriber = pubsub_v1.SubscriberClient()
  subscription_path = subscriber.subscription_path(project, subscription)

  def callback(message):
    print('Received message: {}'.format(message))
    message.ack()

  subscriber.subscribe(subscription_path, callback=callback)

  print('Listening for messages on {}'.format(subscription_path))

  while True:
    time.sleep(60)


if __name__ == '__main__':
  parser = argparse.ArgumentParser(
      description=__doc__,
      formatter_class=argparse.RawDescriptionHelpFormatter
  )
  parser.add_argument('project', help='Your Google Cloud project ID')

  subparsers = parser.add_subparsers(dest='command')

  publish_parser = subparsers.add_parser(
      'publish', help="Publish a single message to a Pub/Sub topic.")
  publish_parser.add_argument('topic')
  publish_parser.add_argument('data')

  subscribe_parser = subparsers.add_parser(
      'subscribe', help="Pull message from Pub/Sub subscription.")
  subscribe_parser.add_argument('subscription')

  args = parser.parse_args()

  if args.command == 'publish':
    publish_message(args.project, args.topic, args.data)
  elif args.command == 'subscribe':
    subscribe_messages(args.project, args.subscription)
