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

import java.net.MalformedURLException;
import java.net.URL;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;

/** Properties that can be set when using Google Cloud Pub/Sub with the Apache Beam SDK. */
@Description(
    "Options that are used to configure Google Cloud Pub/Sub. See "
        + "https://cloud.google.com/pubsub/docs/overview for details on Cloud Pub/Sub.")
public interface PubsubOptions
    extends ApplicationNameOptions, GcpOptions, PipelineOptions, StreamingOptions {

  /**
   * Internal only utility for converting {@link #getPubsubRootUrl()} (e.g. {@code https://<host>})
   * to an endpoint target, usable by GCP client libraries (e.g. {@code <host>:443})
   */
  @Internal
  static String targetForRootUrl(String urlString) {
    URL url;
    try {
      url = new URL(urlString);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(
          String.format("Could not parse pubsub root url \"%s\"", urlString), e);
    }

    int port = url.getPort();

    if (port < 0) {
      switch (url.getProtocol()) {
        case "https":
          port = 443;
          break;
        case "http":
          port = 80;
          break;
        default:
          throw new IllegalArgumentException(
              String.format(
                  "Could not determine port for pubsub root url \"%s\". You must either specify the port or use the protocol \"https\" or \"http\"",
                  urlString));
      }
    }

    return String.format("%s:%d", url.getHost(), port);
  }

  /** Root URL for use with the Google Cloud Pub/Sub API. */
  @Description("Root URL for use with the Google Cloud Pub/Sub API")
  @Default.String("https://pubsub.googleapis.com")
  @Hidden
  String getPubsubRootUrl();

  void setPubsubRootUrl(String value);

  @Description(
      "Whether PubsubUnboundedSource queries Cloud Monitoring for subscription "
          + "num_undelivered_messages to report total backlog records. Requires "
          + "monitoring.timeSeries.list permission on the subscription project.")
  @Default.Boolean(false)
  boolean getPubsubBacklogReportingEnabled();

  void setPubsubBacklogReportingEnabled(boolean value);

  @Description(
      "Whether PubsubUnboundedSource reads with Pub/Sub StreamingPull (google-cloud-pubsub "
          + "Subscriber) instead of unary pull calls. Messages are still acked only when the "
          + "checkpoint covering them is finalized.")
  @Default.Boolean(false)
  boolean getPubsubStreamingPullEnabled();

  void setPubsubStreamingPullEnabled(boolean value);

  @Description(
      "StreamingPull flow control: maximum number of received but not yet acked messages per "
          + "reader, including messages waiting for checkpoint finalization.")
  @Default.Long(20_000L)
  long getPubsubStreamingPullMaxOutstandingMessages();

  void setPubsubStreamingPullMaxOutstandingMessages(long value);

  @Description(
      "StreamingPull flow control: maximum bytes of received but not yet acked messages per "
          + "reader, including messages waiting for checkpoint finalization.")
  @Default.Long(64L * 1024 * 1024)
  long getPubsubStreamingPullMaxOutstandingBytes();

  void setPubsubStreamingPullMaxOutstandingBytes(long value);

  @Description("Number of StreamingPull streams per reader.")
  @Default.Integer(1)
  int getPubsubStreamingPullParallelPullCount();

  void setPubsubStreamingPullParallelPullCount(int value);

  @Description(
      "Maximum total time, in seconds, the StreamingPull subscriber extends a message's lease "
          + "while it waits for the checkpoint covering it to be finalized.")
  @Default.Integer(3600)
  int getPubsubStreamingPullMaxAckExtensionPeriodSeconds();

  void setPubsubStreamingPullMaxAckExtensionPeriodSeconds(int value);
}
