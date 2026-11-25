/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.pubsub;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.core.ApiService;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.IncomingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helpers for the opt-in StreamingPull read path of {@link PubsubUnboundedSource}, enabled with
 * {@link PubsubOptions#getPubsubStreamingPullEnabled()}.
 *
 * <p>The reader uses the high-level {@link Subscriber}, which handles stream reconnects, backoff,
 * flow control and lease (ack deadline) extension. gRPC channels and executors are shared per JVM
 * and per endpoint (reference counted) instead of being created per reader.
 */
class PubsubStreamingPull {
  private static final Logger LOG = LoggerFactory.getLogger(PubsubStreamingPull.class);

  /**
   * Prefix of the ids the StreamingPull path stores in place of Pub/Sub ack ids. The high-level
   * {@link Subscriber} does not expose the real ack id of a message, so each received message gets
   * a reader-local synthetic id, mapped to its live {@link AckReplyConsumer}. Real Pub/Sub ack ids
   * never start with this prefix.
   */
  static final String SYNTHETIC_ACK_ID_PREFIX = "beam-sp:";

  /**
   * Upper bound for a single lease extension. The subscriber keeps extending leases up to {@link
   * PubsubOptions#getPubsubStreamingPullMaxAckExtensionPeriodSeconds()} in total, but each
   * extension is at most this long. If the worker dies without nacking, Pub/Sub redelivers the
   * unacked messages within about this long.
   */
  static final java.time.Duration MAX_DURATION_PER_ACK_EXTENSION = java.time.Duration.ofSeconds(60);

  private static final int MAX_INBOUND_MESSAGE_SIZE = 20 * 1024 * 1024;

  private PubsubStreamingPull() {}

  static boolean isSyntheticAckId(String ackId) {
    return ackId.startsWith(SYNTHETIC_ACK_ID_PREFIX);
  }

  /** A running StreamingPull subscription feeding one reader. */
  interface StreamingPullSubscriber {
    /** Starts receiving. Must not block waiting for messages. */
    void start();

    /**
     * Stops receiving and waits at most {@code timeoutMs} for the subscriber to terminate. The
     * caller must have acked or nacked every message it received before calling this.
     */
    void stop(long timeoutMs);
  }

  /** Creates the {@link StreamingPullSubscriber} for a reader. Replaceable for tests. */
  interface StreamingPullSubscriberFactory extends Serializable {
    StreamingPullSubscriber create(
        SubscriptionPath subscription,
        PubsubOptions options,
        MessageReceiver receiver,
        Consumer<Throwable> onFailure);
  }

  /** The production factory, backed by {@link Subscriber}. */
  static final StreamingPullSubscriberFactory DEFAULT_FACTORY = new SubscriberFactory();

  /**
   * Converts a message received over StreamingPull into an {@link IncomingMessage}, the same way
   * {@link PubsubGrpcClient#pull} does for unary pulls.
   */
  static IncomingMessage toIncomingMessage(
      PubsubMessage pubsubMessage,
      @Nullable String timestampAttribute,
      @Nullable String idAttribute,
      long requestTimeMsSinceEpoch,
      String ackId) {
    Map<String, String> attributes = pubsubMessage.getAttributesMap();

    long timestampMsSinceEpoch;
    if (timestampAttribute == null || timestampAttribute.isEmpty()) {
      Timestamp timestampProto = pubsubMessage.getPublishTime();
      timestampMsSinceEpoch =
          timestampProto.getSeconds() * 1000 + timestampProto.getNanos() / 1000L / 1000L;
    } else {
      timestampMsSinceEpoch =
          PubsubClient.extractTimestampAttribute(timestampAttribute, attributes);
    }

    @Nullable String recordId = idAttribute == null ? null : attributes.get(idAttribute);
    if (recordId == null || recordId.isEmpty()) {
      recordId = pubsubMessage.getMessageId();
    }

    return IncomingMessage.of(
        pubsubMessage, timestampMsSinceEpoch, requestTimeMsSinceEpoch, ackId, recordId);
  }

  /** A message handed to the receiver, waiting to be drained by the reader thread. */
  static final class ReceivedMessage {
    final PubsubMessage message;
    final AckReplyConsumer consumer;

    ReceivedMessage(PubsubMessage message, AckReplyConsumer consumer) {
      this.message = message;
      this.consumer = consumer;
    }
  }

  // ================================================================================
  // Production subscriber
  // ================================================================================

  private static class SubscriberFactory implements StreamingPullSubscriberFactory {
    private static final long serialVersionUID = 1L;

    @Override
    public StreamingPullSubscriber create(
        SubscriptionPath subscription,
        PubsubOptions options,
        MessageReceiver receiver,
        Consumer<Throwable> onFailure) {
      return new SubscriberAdapter(subscription, options, receiver, onFailure);
    }
  }

  private static class SubscriberAdapter implements StreamingPullSubscriber {
    private final SubscriptionPath subscription;
    private final SharedResources resources;
    private final Subscriber subscriber;
    private boolean released = false;

    SubscriberAdapter(
        SubscriptionPath subscription,
        PubsubOptions options,
        MessageReceiver receiver,
        Consumer<Throwable> onFailure) {
      this.subscription = subscription;
      String target = PubsubOptions.targetForRootUrl(options.getPubsubRootUrl());
      // Emulator support: talk plaintext (and without credentials) to http:// or localhost.
      boolean plaintext = options.getPubsubRootUrl().startsWith("http://") || isLocalhost(target);
      this.resources = SharedResources.acquire(target, plaintext);
      try {
        CredentialsProvider credentialsProvider;
        Credentials credentials = plaintext ? null : options.getGcpCredential();
        if (credentials == null) {
          credentialsProvider = NoCredentialsProvider.create();
        } else {
          credentialsProvider = FixedCredentialsProvider.create(credentials);
        }
        this.subscriber =
            Subscriber.newBuilder(subscription.getPath(), receiver)
                .setChannelProvider(
                    FixedTransportChannelProvider.create(
                        GrpcTransportChannel.create(resources.channel)))
                .setCredentialsProvider(credentialsProvider)
                .setExecutorProvider(FixedExecutorProvider.create(resources.executor))
                .setSystemExecutorProvider(FixedExecutorProvider.create(resources.systemExecutor))
                .setParallelPullCount(options.getPubsubStreamingPullParallelPullCount())
                .setFlowControlSettings(
                    FlowControlSettings.newBuilder()
                        .setMaxOutstandingElementCount(
                            options.getPubsubStreamingPullMaxOutstandingMessages())
                        .setMaxOutstandingRequestBytes(
                            options.getPubsubStreamingPullMaxOutstandingBytes())
                        .build())
                .setMaxAckExtensionPeriodDuration(
                    java.time.Duration.ofSeconds(
                        options.getPubsubStreamingPullMaxAckExtensionPeriodSeconds()))
                .setMaxDurationPerAckExtensionDuration(MAX_DURATION_PER_ACK_EXTENSION)
                .build();
      } catch (RuntimeException e) {
        SharedResources.release(resources);
        throw e;
      }
      subscriber.addListener(
          new ApiService.Listener() {
            @Override
            public void failed(ApiService.State from, Throwable failure) {
              LOG.error("StreamingPull subscriber for {} failed", subscription.getPath(), failure);
              onFailure.accept(failure);
            }
          },
          MoreExecutors.directExecutor());
    }

    @Override
    public void start() {
      LOG.info("Starting StreamingPull subscriber for {}", subscription.getPath());
      subscriber.startAsync();
    }

    @Override
    public synchronized void stop(long timeoutMs) {
      if (released) {
        return;
      }
      try {
        subscriber.stopAsync().awaitTerminated(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        LOG.warn(
            "StreamingPull subscriber for {} did not terminate within {} ms",
            subscription.getPath(),
            timeoutMs);
      } catch (IllegalStateException e) {
        // The subscriber had already failed; the failure was reported through the listener.
        LOG.debug("StreamingPull subscriber for {} stopped in failed state", subscription, e);
      } finally {
        release();
      }
    }

    private synchronized void release() {
      if (!released) {
        released = true;
        SharedResources.release(resources);
      }
    }
  }

  private static boolean isLocalhost(String target) {
    return target.startsWith("localhost:")
        || target.startsWith("127.0.0.1:")
        || target.startsWith("[::1]:");
  }

  // ================================================================================
  // Per-JVM shared channel and executors
  // ================================================================================

  /**
   * A gRPC channel and executors shared by every StreamingPull subscriber in this JVM that talks to
   * the same endpoint. Reference counted: created by the first {@link #acquire} and shut down by
   * the last {@link #release}.
   */
  @VisibleForTesting
  static final class SharedResources {
    private static final Map<String, SharedResources> INSTANCES = new HashMap<>();

    final String key;
    final ManagedChannel channel;

    /** Runs message callbacks, lease extension and ack/nack sending for all subscribers. */
    final ScheduledExecutorService executor;

    /** Runs the subscribers' internal alarms. */
    final ScheduledExecutorService systemExecutor;

    private int refCount = 0;

    private SharedResources(String key, ManagedChannel channel) {
      this.key = key;
      this.channel = channel;
      int processors = Runtime.getRuntime().availableProcessors();
      this.executor =
          newExecutor("pubsub-streaming-pull", Math.max(2, Math.min(8, processors / 2)));
      this.systemExecutor =
          newExecutor("pubsub-streaming-pull-system", Math.max(2, Math.min(4, processors / 4)));
    }

    static synchronized SharedResources acquire(String target, boolean plaintext) {
      String key = (plaintext ? "plaintext:" : "tls:") + target;
      SharedResources resources = INSTANCES.get(key);
      if (resources == null) {
        ManagedChannelBuilder<?> builder =
            ManagedChannelBuilder.forTarget(target)
                .maxInboundMessageSize(MAX_INBOUND_MESSAGE_SIZE)
                .keepAliveTime(5, TimeUnit.MINUTES);
        if (plaintext) {
          builder.usePlaintext();
        }
        resources = new SharedResources(key, builder.build());
        INSTANCES.put(key, resources);
        LOG.info("Created shared StreamingPull channel and executors for {}", key);
      }
      resources.refCount++;
      return resources;
    }

    static synchronized void release(SharedResources resources) {
      checkState(resources.refCount > 0, "Released StreamingPull resources too often");
      resources.refCount--;
      if (resources.refCount == 0) {
        INSTANCES.remove(resources.key);
        LOG.info("Shutting down shared StreamingPull channel and executors for {}", resources.key);
        resources.channel.shutdown();
        resources.executor.shutdown();
        resources.systemExecutor.shutdown();
      }
    }

    @VisibleForTesting
    static synchronized int numInstances() {
      return INSTANCES.size();
    }

    private static ScheduledExecutorService newExecutor(String name, int threads) {
      AtomicInteger counter = new AtomicInteger();
      ThreadFactory threadFactory =
          r -> {
            Thread t = new Thread(r, name + "-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
          };
      ScheduledThreadPoolExecutor executor =
          new ScheduledThreadPoolExecutor(threads, threadFactory);
      executor.setRemoveOnCancelPolicy(true);
      return executor;
    }
  }
}
