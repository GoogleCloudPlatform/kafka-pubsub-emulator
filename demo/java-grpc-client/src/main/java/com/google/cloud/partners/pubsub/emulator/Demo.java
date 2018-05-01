package com.google.cloud.partners.pubsub.emulator;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Demo {

  private static final String USAGE =
      "<host:port> publish <topic> -- Publishes messages to <topic>\n" +
          "<host:port> subscribe <subscription> -- Receives and prints messages from <subscription>\n"
          +
          "<host:port> is the hostname and port of the emulator server";
  private static final int SLEEP_1S = 1000;

  private final String target;

  public Demo(String target) {
    this.target = target;
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    if (args.length < 3) {
      System.out.println(USAGE);
    } else {
      Demo demo = new Demo(args[0]);
      if (args[1].equals("publish")) {
        demo.publish(args[2]);
      } else if (args[1].equals("subscribe")) {
        demo.subscribe(args[2]);
      } else {
        System.out.println(USAGE);
      }
    }

  }

  private TransportChannelProvider getChannelProvider() {
    ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext(true).build();
    return FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
  }

  private void subscribe(String subscription) throws InterruptedException {
    System.out.println("Subscribing to messages from " + subscription + "...Press Ctrl-C to exit");
    final CountDownLatch subscribeLatch = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> subscribeLatch.countDown()));

    Subscriber subscriber = Subscriber.newBuilder(subscription,
        (message, ackReplyConsumer) -> {
          ackReplyConsumer.ack();
          System.out.println(
              "Received " + message.getData().toStringUtf8() + " with message-id " + message
                  .getMessageId());
        })
        .setCredentialsProvider(new NoCredentialsProvider())
        .setChannelProvider(getChannelProvider())
        .build();
    subscriber.startAsync().awaitRunning();
    subscribeLatch.await();
  }

  private void publish(String topic) throws IOException, InterruptedException {
    System.out.println("Publishing messages to " + topic + "...Press Ctrl-C to exit");
    Publisher publisher = Publisher.newBuilder(topic)
        .setCredentialsProvider(new NoCredentialsProvider())
        .setChannelProvider(getChannelProvider())
        .build();
    int messageNo = 1;
    while (true) {
      String message = "Message #" + messageNo++;
      ApiFuture<String> publishFuture = publisher.publish(PubsubMessage.newBuilder()
          .setData(ByteString.copyFromUtf8(message))
          .build());
      ApiFutures.addCallback(publishFuture, new ApiFutureCallback<String>() {
        @Override
        public void onFailure(Throwable throwable) {
          System.err.println("Error publishing " + message);
        }

        @Override
        public void onSuccess(String messageId) {
          System.out.println("Published " + message + " with message-id " + messageId);
        }
      });
      Thread.sleep(SLEEP_1S);
    }
  }
}
