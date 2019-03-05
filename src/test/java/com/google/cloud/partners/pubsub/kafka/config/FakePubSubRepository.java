package com.google.cloud.partners.pubsub.kafka.config;

import com.google.cloud.partners.pubsub.kafka.TestHelpers;

/** Fake repository used for testing. Latest state is saved in an attribute. */
public class FakePubSubRepository implements PubSubRepository {

  private final PubSub onLoad;
  private PubSub lastSaved = null;

  public FakePubSubRepository() {
    onLoad = TestHelpers.PUBSUB_CONFIG;
  }

  public FakePubSubRepository(PubSub onLoad) {
    this.onLoad = onLoad;
  }

  public PubSub getLastSaved() {
    return lastSaved;
  }

  @Override
  public PubSub load() {
    return onLoad;
  }

  @Override
  public void save(PubSub pubSub) {
    lastSaved = pubSub;
  }
}
