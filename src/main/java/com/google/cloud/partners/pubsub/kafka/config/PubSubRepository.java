package com.google.cloud.partners.pubsub.kafka.config;

/**
 * Interface to describe a repository for retrieving and saving {@link PubSub} application-level
 * configuration.
 */
public interface PubSubRepository {

  /** Retrieves the PubSub application configuration from its store. */
  PubSub load();

  /** Persists the managed PubSub application configuration to its store. */
  void save(PubSub pubSub);
}
