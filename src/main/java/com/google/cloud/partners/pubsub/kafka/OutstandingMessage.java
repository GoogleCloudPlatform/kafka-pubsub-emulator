/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.partners.pubsub.kafka;

import java.time.Instant;

public final class OutstandingMessage {

  private final int partition;
  private final long offset;
  private final String messageId;
  private final Instant pulledAt;
  private Instant expiresAt; // Expiration can be changed, so this is not final
  private boolean acknowledged;

  private OutstandingMessage(Builder builder) {
    partition = builder.partition;
    offset = builder.offset;
    pulledAt = builder.pulledAt;
    expiresAt = builder.expiresAt;
    setAcknowledged(builder.acknowledged);

    messageId = partition + "-" + offset;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Returns true if this object's expiration timestamp occurs after {@code checkTime}
   *
   * @param checkTime Instant to check
   */
  public boolean isExpired(Instant checkTime) {
    return checkTime.isAfter(expiresAt);
  }

  public boolean isAcknowledged() {
    return acknowledged;
  }

  public void setAcknowledged(boolean acknowledged) {
    this.acknowledged = acknowledged;
  }

  /** Adds {@code seconds} to the expiration timestamp and returns the new value. */
  public Instant addSecondsToDeadline(int seconds) {
    expiresAt = expiresAt.plusSeconds(seconds);
    return expiresAt;
  }

  public String getMessageId() {
    return messageId;
  }

  public Instant getPulledAt() {
    return pulledAt;
  }

  public Instant getExpiresAt() {
    return expiresAt;
  }

  public int getPartition() {
    return partition;
  }

  public long getOffset() {
    return offset;
  }

  @Override
  public String toString() {
    return "OutstandingMessage{"
        + "messageId='"
        + messageId
        + '\''
        + ", pulledAt="
        + pulledAt
        + ", expiresAt="
        + expiresAt
        + '}';
  }

  /** {@code OutstandingMessage} builder static inner class. */
  public static final class Builder {

    private Instant pulledAt;
    private Instant expiresAt;
    private boolean acknowledged;
    private int partition;
    private long offset;

    private Builder() {}

    /**
     * Sets the {@code messageId} and returns a reference to this Builder so that the methods can be
     * chained together.
     *
     * @param val the {@code messageId} to set
     * @return a reference to this Builder
     */
    public Builder setMessageId(String val) {
      String[] pieces = val.split("-");
      try {
        partition = Integer.parseInt(pieces[0]);
        offset = Long.parseLong(pieces[1]);
        return this;
      } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
        throw new IllegalArgumentException("Message Id must be <partition>-<offset>");
      }
    }

    /**
     * Sets the {@code pulledAt} and returns a reference to this Builder so that the methods can be
     * chained together.
     *
     * @param val the {@code pulledAt} to set
     * @return a reference to this Builder
     */
    public Builder setPulledAt(Instant val) {
      pulledAt = val;
      return this;
    }

    /**
     * Sets the {@code expiresAt} and returns a reference to this Builder so that the methods can be
     * chained together.
     *
     * @param val the {@code expiresAt} to set
     * @return a reference to this Builder
     */
    public Builder setExpiresAt(Instant val) {
      expiresAt = val;
      return this;
    }

    /**
     * Sets the {@code acknowledged} and returns a reference to this Builder so that the methods can
     * be chained together.
     *
     * @param val the {@code acknowledged} to set
     * @return a reference to this Builder
     */
    public Builder setAcknowledged(boolean val) {
      acknowledged = val;
      return this;
    }

    /**
     * Returns a {@code OutstandingMessage} built from the parameters previously set.
     *
     * @return a {@code OutstandingMessage} built with parameters of this {@code
     *     OutstandingMessage.Builder}
     */
    public OutstandingMessage build() {
      return new OutstandingMessage(this);
    }

    /**
     * Sets the {@code partition} and returns a reference to this Builder so that the methods can be
     * chained together.
     *
     * @param val the {@code partition} to set
     * @return a reference to this Builder
     */
    public Builder setPartition(int val) {
      partition = val;
      return this;
    }

    /**
     * Sets the {@code offset} and returns a reference to this Builder so that the methods can be
     * chained together.
     *
     * @param val the {@code offset} to set
     * @return a reference to this Builder
     */
    public Builder setOffset(long val) {
      offset = val;
      return this;
    }
  }
}
