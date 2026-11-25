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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.util.Clock;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.StreamingPullRequest;
import com.google.pubsub.v1.StreamingPullResponse;
import com.google.pubsub.v1.SubscriberGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.PubsubClientFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubStreamingPull.SharedResources;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubStreamingPull.StreamingPullSubscriber;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubStreamingPull.StreamingPullSubscriberFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSource.PubsubCheckpoint;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSource.PubsubReader;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubUnboundedSource.PubsubSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests the StreamingPull read path of {@link PubsubUnboundedSource}. */
@RunWith(JUnit4.class)
public class PubsubUnboundedSourceStreamingPullTest {
  private static final SubscriptionPath SUBSCRIPTION =
      PubsubClient.subscriptionPathFromName("testProject", "testSubscription");
  private static final String TIMESTAMP_ATTRIBUTE = "timestamp";
  private static final String ID_ATTRIBUTE = "id";
  private static final long START_TIME = 100_000L;

  /** The fake subscriber the reader under test created; set by {@link FakeSubscriberFactory}. */
  private static volatile @Nullable FakeSubscriber lastSubscriber;

  /** The mocked unary client every reader under test uses. */
  private static volatile @Nullable PubsubClient unaryClient;

  private AtomicLong now;
  private PubsubOptions options;

  @Before
  public void setUp() throws IOException {
    now = new AtomicLong(START_TIME);
    lastSubscriber = null;
    PubsubClient client = mock(PubsubClient.class);
    when(client.ackDeadlineSeconds(any())).thenReturn(60);
    unaryClient = client;
    options = PipelineOptionsFactory.create().as(PubsubOptions.class);
    options.setPubsubStreamingPullEnabled(true);
  }

  @After
  public void tearDown() {
    lastSubscriber = null;
    unaryClient = null;
  }

  // ================================================================================
  // Fakes
  // ================================================================================

  /** Hands out the static {@link #unaryClient}. */
  private static class MockClientFactory implements PubsubClientFactory {
    @Override
    public PubsubClient newClient(
        @Nullable String timestampAttribute, @Nullable String idAttribute, PubsubOptions options) {
      return unaryClient;
    }

    @Override
    public PubsubClient newClient(
        @Nullable String timestampAttribute,
        @Nullable String idAttribute,
        PubsubOptions options,
        @Nullable String rootUrlOverride) {
      return unaryClient;
    }

    @Override
    public String getKind() {
      return "Mock";
    }
  }

  /** What happened to a message handed to the receiver. */
  private static class FakeConsumer implements AckReplyConsumer {
    final String messageId;
    volatile @Nullable String reply;

    FakeConsumer(String messageId) {
      this.messageId = messageId;
    }

    @Override
    public synchronized void ack() {
      if (reply == null) {
        reply = "ack";
      }
    }

    @Override
    public synchronized void nack() {
      if (reply == null) {
        reply = "nack";
      }
    }
  }

  private static class FakeSubscriber implements StreamingPullSubscriber {
    final MessageReceiver receiver;
    final Consumer<Throwable> onFailure;
    final List<FakeConsumer> consumers = new CopyOnWriteArrayList<>();
    volatile boolean started = false;
    volatile boolean stopped = false;

    FakeSubscriber(MessageReceiver receiver, Consumer<Throwable> onFailure) {
      this.receiver = receiver;
      this.onFailure = onFailure;
    }

    @Override
    public void start() {
      started = true;
    }

    @Override
    public void stop(long timeoutMs) {
      stopped = true;
    }

    FakeConsumer deliver(PubsubMessage message) {
      FakeConsumer consumer = new FakeConsumer(message.getMessageId());
      consumers.add(consumer);
      receiver.receiveMessage(message, consumer);
      return consumer;
    }

    List<String> replied(String reply) {
      List<String> ids = new ArrayList<>();
      for (FakeConsumer consumer : consumers) {
        if (reply.equals(consumer.reply)) {
          ids.add(consumer.messageId);
        }
      }
      return ids;
    }
  }

  private static class FakeSubscriberFactory implements StreamingPullSubscriberFactory {
    @Override
    public StreamingPullSubscriber create(
        SubscriptionPath subscription,
        PubsubOptions options,
        MessageReceiver receiver,
        Consumer<Throwable> onFailure) {
      assertEquals(SUBSCRIPTION, subscription);
      FakeSubscriber subscriber = new FakeSubscriber(receiver, onFailure);
      lastSubscriber = subscriber;
      return subscriber;
    }
  }

  /** Fails if the reader tries to create a StreamingPull subscriber. */
  private static class ForbiddenSubscriberFactory implements StreamingPullSubscriberFactory {
    @Override
    public StreamingPullSubscriber create(
        SubscriptionPath subscription,
        PubsubOptions options,
        MessageReceiver receiver,
        Consumer<Throwable> onFailure) {
      throw new AssertionError("StreamingPull must not be used when the option is off");
    }
  }

  private PubsubSource newSource(StreamingPullSubscriberFactory subscriberFactory) {
    Clock clock = () -> now.get();
    PubsubUnboundedSource source =
        new PubsubUnboundedSource(
            clock,
            new MockClientFactory(),
            null,
            null,
            StaticValueProvider.of(SUBSCRIPTION),
            TIMESTAMP_ATTRIBUTE,
            ID_ATTRIBUTE,
            true /* needsAttributes */);
    source.setStreamingPullSubscriberFactory(subscriberFactory);
    return new PubsubSource(source);
  }

  private static PubsubMessage message(String messageId, long timestampMs) {
    return PubsubMessage.newBuilder()
        .setMessageId(messageId)
        .setData(ByteString.copyFromUtf8("data-" + messageId))
        .putAttributes(TIMESTAMP_ATTRIBUTE, String.valueOf(timestampMs))
        .putAttributes(ID_ATTRIBUTE, "record-" + messageId)
        .build();
  }

  private static String data(byte[] payload) throws Exception {
    return PubsubMessage.parseFrom(payload).getData().toStringUtf8();
  }

  private static List<String> readAll(PubsubReader reader) throws Exception {
    List<String> read = new ArrayList<>();
    while (reader.advance()) {
      read.add(data(reader.getCurrent()));
    }
    return read;
  }

  // ================================================================================
  // Tests
  // ================================================================================

  @Test
  public void deliversWithoutBlockingOrUnaryPulls() throws Exception {
    PubsubReader reader = newSource(new FakeSubscriberFactory()).createReader(options, null);
    // Nothing received yet: start() and advance() return immediately.
    assertFalse(reader.start());
    FakeSubscriber subscriber = lastSubscriber;
    assertNotNull(subscriber);
    assertTrue(subscriber.started);
    assertFalse(reader.advance());

    subscriber.deliver(message("m1", 1111L));
    assertTrue(reader.advance());
    assertEquals("data-m1", data(reader.getCurrent()));
    assertEquals(new Instant(1111L), reader.getCurrentTimestamp());
    assertEquals("record-m1", new String(reader.getCurrentRecordId(), StandardCharsets.UTF_8));
    assertFalse(reader.advance());

    verify(unaryClient, never()).pull(anyLong(), any(), anyInt(), anyBoolean());
    verify(unaryClient, never()).modifyAckDeadline(any(), any(), anyInt());
    reader.close();
  }

  @Test
  public void fallsBackToPublishTimeAndMessageId() throws Exception {
    PubsubUnboundedSource source =
        new PubsubUnboundedSource(
            () -> now.get(),
            new MockClientFactory(),
            null,
            null,
            StaticValueProvider.of(SUBSCRIPTION),
            null /* timestampAttribute */,
            null /* idAttribute */,
            false /* needsAttributes */);
    source.setStreamingPullSubscriberFactory(new FakeSubscriberFactory());
    PubsubReader reader = new PubsubSource(source).createReader(options, null);
    assertFalse(reader.start());
    lastSubscriber.deliver(
        PubsubMessage.newBuilder()
            .setMessageId("m1")
            .setData(ByteString.copyFromUtf8("payload"))
            .setPublishTime(Timestamp.newBuilder().setSeconds(5).setNanos(7_000_000))
            .build());
    assertTrue(reader.advance());
    assertEquals("payload", new String(reader.getCurrent(), StandardCharsets.UTF_8));
    assertEquals(new Instant(5007L), reader.getCurrentTimestamp());
    assertEquals("m1", new String(reader.getCurrentRecordId(), StandardCharsets.UTF_8));
    reader.close();
  }

  @Test
  public void acksOnlyWhenCheckpointIsFinalized() throws Exception {
    PubsubReader reader = newSource(new FakeSubscriberFactory()).createReader(options, null);
    assertFalse(reader.start());
    FakeSubscriber subscriber = lastSubscriber;
    subscriber.deliver(message("m1", 1L));
    subscriber.deliver(message("m2", 2L));
    assertThat(readAll(reader), containsInAnyOrder("data-m1", "data-m2"));
    assertThat(subscriber.replied("ack"), empty());

    PubsubCheckpoint first = reader.getCheckpointMark();
    // Read after the checkpoint: not covered by it.
    subscriber.deliver(message("m3", 3L));
    assertThat(readAll(reader), containsInAnyOrder("data-m3"));
    assertThat(subscriber.replied("ack"), empty());

    first.finalizeCheckpoint();
    assertThat(subscriber.replied("ack"), containsInAnyOrder("m1", "m2"));

    PubsubCheckpoint second = reader.getCheckpointMark();
    second.finalizeCheckpoint();
    assertThat(subscriber.replied("ack"), containsInAnyOrder("m1", "m2", "m3"));
    assertThat(subscriber.replied("nack"), empty());

    // Acks go over the subscriber, never through the unary client.
    verify(unaryClient, never()).acknowledge(any(), any());
    reader.close();
    assertTrue(subscriber.stopped);
  }

  @Test
  public void closeNacksEverythingNotFinalized() throws Exception {
    PubsubReader reader = newSource(new FakeSubscriberFactory()).createReader(options, null);
    assertFalse(reader.start());
    FakeSubscriber subscriber = lastSubscriber;
    for (int i = 1; i <= 5; i++) {
      subscriber.deliver(message("m" + i, i));
    }
    // m1 is read and in a finalized checkpoint, m2 is read and in a checkpoint that is never
    // finalized, m3 is read but not checkpointed, m4 and m5 are not read yet.
    assertTrue(reader.advance());
    reader.getCheckpointMark().finalizeCheckpoint();
    assertTrue(reader.advance());
    PubsubCheckpoint pending = reader.getCheckpointMark();
    assertTrue(reader.advance());

    reader.close();
    assertTrue(subscriber.stopped);
    assertThat(subscriber.replied("ack"), containsInAnyOrder("m1"));
    assertThat(subscriber.replied("nack"), containsInAnyOrder("m2", "m3", "m4", "m5"));

    // A late finalization doesn't ack what was already nacked.
    pending.finalizeCheckpoint();
    assertThat(subscriber.replied("ack"), containsInAnyOrder("m1"));

    // Messages arriving after close are nacked right away.
    FakeConsumer late = subscriber.deliver(message("m6", 6L));
    assertEquals("nack", late.reply);
  }

  @Test
  public void restoredCheckpointNacksRealAckIdsThroughUnaryClient() throws Exception {
    PubsubSource source = newSource(new FakeSubscriberFactory());
    PubsubReader reader = source.createReader(options, null);
    assertFalse(reader.start());
    lastSubscriber.deliver(message("m1", 1L));
    lastSubscriber.deliver(message("m2", 2L));
    assertTrue(reader.advance());
    // m2 is not yet read, so it is in the checkpoint's notYetReadIds as a synthetic id.
    PubsubCheckpoint checkpoint = reader.getCheckpointMark();
    assertThat(checkpoint.notYetReadIds, hasSize(1));
    assertTrue(PubsubStreamingPull.isSyntheticAckId(checkpoint.notYetReadIds.get(0)));

    // A checkpoint taken by a unary-pull reader holds real Pub/Sub ack ids. Mix both.
    List<String> notYetRead = new ArrayList<>(checkpoint.notYetReadIds);
    notYetRead.add("real-ack-id");
    PubsubCheckpoint restored =
        CoderUtils.clone(
            source.getCheckpointMarkCoder(), new PubsubCheckpoint(null, null, null, notYetRead));

    PubsubReader restoredReader = source.createReader(options, restored);
    // Only the real id goes to Pub/Sub; the synthetic one belonged to a subscriber that is gone.
    verify(unaryClient).modifyAckDeadline(SUBSCRIPTION, ImmutableList.of("real-ack-id"), 0);
    verify(unaryClient, never()).modifyAckDeadline(any(), eq(checkpoint.notYetReadIds), anyInt());

    // Acks of real ids (no live consumer) also go through the unary client.
    restoredReader.ackBatch(new ArrayList<>(ImmutableList.of("real-ack-id-2")));
    verify(unaryClient).acknowledge(SUBSCRIPTION, ImmutableList.of("real-ack-id-2"));
    reader.close();
    restoredReader.close();
  }

  @Test
  public void watermarkAndBacklogBookkeeping() throws Exception {
    PubsubReader reader = newSource(new FakeSubscriberFactory()).createReader(options, null);
    assertFalse(reader.start());
    FakeSubscriber subscriber = lastSubscriber;
    assertEquals(BoundedWindow.TIMESTAMP_MIN_VALUE, reader.getWatermark());
    // Backlog reporting is off by default.
    assertEquals(UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN, reader.getTotalBacklogRecords());

    long minTimestamp = 50_000L;
    int n = 100;
    for (int i = 0; i < n; i++) {
      subscriber.deliver(message("m" + i, minTimestamp + i));
    }
    // Received messages are drained into the unread queue on the first advance.
    assertTrue(reader.advance());
    long messageBytes = ("data-m" + 0).length();
    assertThat(reader.getSplitBacklogBytes(), greaterThanOrEqualTo((n - 1) * messageBytes));
    for (int i = 1; i < n; i++) {
      now.addAndGet(200);
      assertTrue(reader.advance());
      long watermark = reader.getWatermark().getMillis();
      assertThat(watermark, lessThanOrEqualTo(minTimestamp + i));
    }
    assertFalse(reader.advance());
    assertEquals(0L, reader.getSplitBacklogBytes());
    assertThat(reader.getWatermark().getMillis(), greaterThanOrEqualTo(minTimestamp));

    // Idle for longer than the sample period: the watermark advances to now.
    now.addAndGet(5 * 60 * 1000);
    assertFalse(reader.advance());
    assertEquals(new Instant(now.get()), reader.getWatermark());
    reader.close();
  }

  @Test
  public void subscriberFailureFailsReader() throws Exception {
    PubsubReader reader = newSource(new FakeSubscriberFactory()).createReader(options, null);
    assertFalse(reader.start());
    lastSubscriber.onFailure.accept(new RuntimeException("PERMISSION_DENIED"));
    IOException e = assertThrows(IOException.class, reader::advance);
    assertEquals("PERMISSION_DENIED", e.getCause().getMessage());
    reader.close();
    assertTrue(lastSubscriber.stopped);
  }

  @Test
  public void optionOffKeepsUnaryPullAndScaleOut() throws Exception {
    PubsubOptions offOptions = PipelineOptionsFactory.create().as(PubsubOptions.class);
    assertFalse(offOptions.getPubsubStreamingPullEnabled());
    PubsubSource source = newSource(new ForbiddenSubscriberFactory());
    assertThat(source.split(3, offOptions), hasSize(12));
    assertThat(source.split(3, options), hasSize(3));

    when(unaryClient.pull(anyLong(), any(), anyInt(), anyBoolean()))
        .thenReturn(Collections.emptyList());
    PubsubReader reader = source.createReader(offOptions, null);
    assertFalse(reader.start());
    assertFalse(reader.advance());
    verify(unaryClient, times(2)).pull(anyLong(), eq(SUBSCRIPTION), anyInt(), eq(true));
    reader.close();
  }

  @Test
  public void sharedResourcesAreReferenceCounted() {
    int before = SharedResources.numInstances();
    SharedResources a = SharedResources.acquire("localhost:1", true);
    SharedResources b = SharedResources.acquire("localhost:1", true);
    SharedResources c = SharedResources.acquire("localhost:2", true);
    assertSame(a, b);
    assertEquals(before + 2, SharedResources.numInstances());
    SharedResources.release(a);
    assertFalse(a.channel.isShutdown());
    SharedResources.release(b);
    assertTrue(a.channel.isShutdown());
    assertTrue(a.executor.isShutdown());
    SharedResources.release(c);
    assertEquals(before, SharedResources.numInstances());
  }

  // ================================================================================
  // End to end with the real Subscriber against a local fake Pub/Sub server
  // ================================================================================

  /** Minimal StreamingPull server: hands out messages and records acks and nacks. */
  private static class FakePubsubServer extends SubscriberGrpc.SubscriberImplBase {
    final Map<String, String> replies = new ConcurrentHashMap<>();
    final List<StreamObserver<StreamingPullResponse>> streams = new CopyOnWriteArrayList<>();

    @Override
    public StreamObserver<StreamingPullRequest> streamingPull(
        StreamObserver<StreamingPullResponse> responseObserver) {
      return new StreamObserver<StreamingPullRequest>() {
        @Override
        public void onNext(StreamingPullRequest request) {
          if (!request.getSubscription().isEmpty()) {
            streams.add(responseObserver);
          }
          request.getAckIdsList().forEach(id -> replies.put(id, "ack"));
          for (int i = 0; i < request.getModifyDeadlineAckIdsCount(); i++) {
            if (request.getModifyDeadlineSeconds(i) == 0) {
              replies.put(request.getModifyDeadlineAckIds(i), "nack");
            }
          }
        }

        @Override
        public void onError(Throwable t) {
          streams.remove(responseObserver);
        }

        @Override
        public void onCompleted() {
          streams.remove(responseObserver);
          responseObserver.onCompleted();
        }
      };
    }

    @Override
    public void acknowledge(AcknowledgeRequest request, StreamObserver<Empty> responseObserver) {
      request.getAckIdsList().forEach(id -> replies.put(id, "ack"));
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void modifyAckDeadline(
        ModifyAckDeadlineRequest request, StreamObserver<Empty> responseObserver) {
      if (request.getAckDeadlineSeconds() == 0) {
        request.getAckIdsList().forEach(id -> replies.put(id, "nack"));
      }
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    }

    void send(String ackId, PubsubMessage message) {
      StreamingPullResponse response =
          StreamingPullResponse.newBuilder()
              .addReceivedMessages(
                  ReceivedMessage.newBuilder().setAckId(ackId).setMessage(message).build())
              .build();
      StreamObserver<StreamingPullResponse> stream = streams.get(0);
      synchronized (stream) {
        stream.onNext(response);
      }
    }
  }

  private static void waitFor(String what, BooleanSupplier condition) throws Exception {
    long deadline = System.currentTimeMillis() + 30_000;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline) {
        throw new AssertionError("Timed out waiting for " + what);
      }
      Thread.sleep(20);
    }
  }

  @Test
  public void endToEndWithRealSubscriber() throws Exception {
    FakePubsubServer service = new FakePubsubServer();
    Server server = ServerBuilder.forPort(0).addService(service).build().start();
    try {
      options.setPubsubRootUrl("http://localhost:" + server.getPort());
      PubsubUnboundedSource source =
          new PubsubUnboundedSource(
              () -> now.get(),
              new MockClientFactory(),
              null,
              null,
              StaticValueProvider.of(SUBSCRIPTION),
              TIMESTAMP_ATTRIBUTE,
              ID_ATTRIBUTE,
              true /* needsAttributes */);
      PubsubReader reader = new PubsubSource(source).createReader(options, null);
      assertFalse(reader.start());
      waitFor("stream to open", () -> !service.streams.isEmpty());

      service.send("ack-1", message("m1", 1L));
      service.send("ack-2", message("m2", 2L));
      List<String> read = new ArrayList<>();
      waitFor(
          "messages to be read",
          () -> {
            try {
              while (reader.advance()) {
                read.add(data(reader.getCurrent()));
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
            return read.size() == 2;
          });
      assertThat(read, containsInAnyOrder("data-m1", "data-m2"));

      PubsubCheckpoint checkpoint = reader.getCheckpointMark();
      Thread.sleep(500);
      assertFalse("acked before finalize", service.replies.containsValue("ack"));
      checkpoint.finalizeCheckpoint();
      waitFor("acks", () -> service.replies.size() == 2);
      assertEquals(ImmutableMap.of("ack-1", "ack", "ack-2", "ack"), service.replies);

      // Read but never checkpointed, so nacked on close.
      service.send("ack-3", message("m3", 3L));
      waitFor(
          "m3 to be read",
          () -> {
            try {
              return reader.advance();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
      reader.close();
      waitFor("nack", () -> "nack".equals(service.replies.get("ack-3")));
      verify(unaryClient, never()).pull(anyLong(), any(), anyInt(), anyBoolean());
      verify(unaryClient, never()).acknowledge(any(), any());
      verify(unaryClient, never()).modifyAckDeadline(any(), any(), anyInt());
    } finally {
      server.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
    }
  }
}
