# Demo Program using the Python PubSub REST Client Library

## Usage
To use Cloud Pub/Sub, you will need to create an OAuth 2.0 client ID for an
installed application and download the client secret JSON file to the same
folder as this program. It must be named `client_secret.json`.

To install in a virtual environment:
```
virtualenv .env
. .env/bin/activate
pip install -r requirements.txt
```

## Publishing
Then, to publish to a Cloud PubSub topic, issue the following command where
**<project>** is the name of your GCP project, and **<topic>** is the topic
you wish to publish to. You will be prompted to authorize the request the
first time you start the program.

`python main.py publish <project> --topic <topic>`

If you wish to use the Emulator gateway, simply provide its hostname and port with the `--gateway`
argument.

## Subscribing
To Pull messages from a Cloud PubSub subscription, issue the following command
where **<project>** is the name of your GCP project, and **<subscription>** is
the subscription you wish to pull from.

`python main.py subscribe <project> --subscription <subscription>`

If you wish to use the Emulator gateway, simply provide its hostname and port with the `--gateway`
argument.

