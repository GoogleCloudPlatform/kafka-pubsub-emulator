package com.google.cloud.partners.pubsub.kafka.properties;

public class PubSubBindProperties {

  private String topic;

  private String subscription;

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getSubscription() {
    return subscription;
  }

  public void setSubscription(String subscription) {
    this.subscription = subscription;
  }

  @Override
  public String toString() {
    return "PubSubBindProperties{" +
        "topic='" + topic + '\'' +
        ", subscription='" + subscription + '\'' +
        '}';
  }
}
