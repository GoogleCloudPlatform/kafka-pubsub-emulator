# Demo Program using the Python PubSub gRPC Client Library

## Usage
To use Cloud Pub/Sub, requires you to have authentication setup. 
Refer to the [Authentication Getting Started Guide](https://cloud.google.com/docs/authentication/getting-started) 
for instructions on setting up credentials.

If you have a service account you can configure on a environment:
```
export GOOGLE_APPLICATION_CREDENTIALS=path/to/your/service_account.json
```

To install in a virtual environment:
```
virtualenv .env
. .env/bin/activate
pip install -r requirements.txt
```

(Optional)To connect on a Pub/Sub emulator:
```
export PUBSUB_EMULATOR_HOST=<host>:<port>
```

## Publishing

Then, to publish to a Cloud PubSub topic, issue the following command where
**<project>** is the name of your GCP project, **<topic>** is the topic
you wish to publish to, **<data>** is the message content.

`python main.py <project> publish <topic> <data>`

## Subscribing

To Pull messages from a Cloud PubSub subscription, issue the following command
where **<project>** is the name of your GCP project, and **<subscription>** is
the subscription you wish to pull from.

`python main.py <project> subscribe <subscription>`