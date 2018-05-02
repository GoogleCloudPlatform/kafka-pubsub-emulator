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
from base64 import b64decode, b64encode
import logging
import time

from googleapiclient import discovery
from googleapiclient.http import build_http
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools

API_SERVICE_NAME = 'pubsub'
API_VERSION = 'v1'
CLIENT_SECRETS = 'client_secret.json'
CREDENTIAL_FILE = '.credential.dat'
SCOPES = ('https://www.googleapis.com/auth/cloud-platform',
          'https://www.googleapis.com/auth/pubsub')
TOPIC_FORMAT = 'projects/{project}/topics/{topic}'
SUBSCRIPTION_FORMAT = 'projects/{project}/subscriptions/{sub}'

# Map standard API object keys to underscore_cased keys used by the gRPC gateway
MESSAGE_IDS = 'messageIds'
RECEIVED_MESSAGES = 'receivedMessages'
ACK_ID = 'ackId'

parser = argparse.ArgumentParser(
    description='Demo program with Pub/Sub', parents=[tools.argparser])
parser.add_argument(
    'action', choices=('publish', 'subscribe'), help='Publish or Subscribe')
parser.add_argument('project', help='GCP Project Id')
parser.add_argument('--gateway', help='Hostname and port of Emulator gateway')
parser.add_argument('--topic', help='Topic to Publish to')
parser.add_argument('--subscription', help='Subscription to Pull from')

logger = logging.getLogger('pubsub_demo')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class InterceptingHttp(httplib2.Http):
  """Intercepts and rewrites the PubSub URI with the Gateway's."""

  PUBSUB_URI = 'https://pubsub.googleapis.com'

  def __init__(self, gateway_uri=None, **kwargs):
    super(self.__class__, self).__init__(**kwargs)
    assert gateway_uri
    self.__gateway_uri = gateway_uri

  def request(self,
              uri,
              method='GET',
              body=None,
              headers=None,
              redirections=httplib2.DEFAULT_MAX_REDIRECTS,
              connection_type=None):
    orig = uri
    uri = uri.replace(self.__class__.PUBSUB_URI, self.__gateway_uri)
    logger.debug('Rewriting URI from %s to %s', orig, uri)
    return super(self.__class__, self).request(uri, method, body, headers,
                                               redirections, connection_type)


def _get_authenticated_service(args):
  flow = client.flow_from_clientsecrets(CLIENT_SECRETS, scope=SCOPES)
  storage = file.Storage(CREDENTIAL_FILE)
  credentials = storage.get()
  if credentials is None or credentials.invalid:
    credentials = tools.run_flow(flow, storage, flags=args)

  if args.gateway:
    logger.info('Using intercepting Http class with %s', args.gateway)
    http = InterceptingHttp(args.gateway)
  else:
    http = credentials.authorize(http=build_http())

  service = discovery.build(API_SERVICE_NAME, API_VERSION, http=http)
  return service


def _publish(pubsub, project, topic):
  seq_no = 0
  data = 'Message SeqNo {}'
  topic_name = TOPIC_FORMAT.format(project=project, topic=topic)
  while True:
    message = {
        'messages': [{
            'attributes': {
                'sentAt': str(int(time.time()))
            },
            'data': b64encode(data.format(seq_no).encode()).decode()
        }]
    }
    response = pubsub.projects().topics().publish(
        topic=topic_name, body=message).execute()
    # Try to get the standard messageIds attribute, if None,
    # get the underscore case used by the gateway
    message_id = response.get(MESSAGE_IDS)
    logger.info('Published message with Id: {}'.format(message_id))
    seq_no += 1
    time.sleep(0.5)


def _pull(pubsub, project, subscription):
  subscription_name = SUBSCRIPTION_FORMAT.format(
      project=project, sub=subscription)
  while True:
    response = pubsub.projects().subscriptions().pull(
        subscription=subscription_name, body={
            'maxMessages': 5
        }).execute()
    acks = []
    received_messages = response.get(RECEIVED_MESSAGES)
    if received_messages:
      for m in received_messages:
        logger.info('Received {}'.format(m.get('message')))
        ack_id = m.get(ACK_ID)
        acks.append(ack_id)
      pubsub.projects().subscriptions().acknowledge(
          subscription=subscription_name, body={
              'ackIds': acks
          }).execute()
      logger.info('Acknowledged {} messages'.format(len(acks)))
    time.sleep(0.5)


def main():
  args = parser.parse_args()
  pubsub = _get_authenticated_service(args)

  if args.action == 'publish':
    assert args.topic
    _publish(pubsub, args.project, args.topic)
  elif args.action == 'subscribe':
    assert args.subscription
    _pull(pubsub, args.project, args.subscription)


if __name__ == '__main__':
  main()
