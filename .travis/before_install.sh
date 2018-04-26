#!/usr/bin/env bash

# Decrypt the service account key, ensure gcloud is install and activate the service account
openssl aes-256-cbc -K $encrypted_865dee0081bc_key -iv $encrypted_865dee0081bc_iv -in \
  kafka-pubsub-emulator_svc_acct.json.enc -out kafka-pubsub-emulator_svc_acct.json -d

gcloud version || echo "gcloud not installed"
if [ ! -d "$HOME/google-cloud-sdk/bin" ]; then
    rm -rf $HOME/google-cloud-sdk;
    curl https://sdk.cloud.google.com | bash;
fi
gcloud auth activate-service-account --key-file kafka-pubsub-emulator_svc_acct.json
gcloud config set project kafka-pubsub-emulator
