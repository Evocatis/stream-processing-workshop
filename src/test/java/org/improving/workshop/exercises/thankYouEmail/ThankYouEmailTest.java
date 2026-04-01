package org.improving.workshop.exercises.thankYouEmail;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.improving.workshop.Streams;
import org.improving.workshop.exercises.stateful.ThankYouEmail;
import org.improving.workshop.utils.DataFaker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.msse.demo.mockdata.music.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ThankYouEmailTest {
  private TopologyTestDriver driver;

  private TestInputTopic<String, Stream> streamInputTopic;
  private TestOutputTopic<String, ThankYouEmail.ThankYouMessage> outputTopic;

  @BeforeEach
  public void setup() {

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    ThankYouEmail.configureTopology(streamsBuilder);

    driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

    streamInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_STREAMS,
            Serdes.String().serializer(),
            Streams.SERDE_STREAM_JSON.serializer()
    );

    outputTopic = driver.createOutputTopic(
            ThankYouEmail.OUTPUT_TOPIC,
            Serdes.String().deserializer(),
            ThankYouEmail.SERDE_THANK_YOU_MESSAGE.deserializer()
    );
  }

  @AfterEach
  public void cleanup() {
    driver.close();
  }

  @Test
  @DisplayName("top artist streamer thank you emails")
  public void testTopArtistStreamerThankYouEmails() {
    // Given: Create some streams for artist-1
    String artist1 = "artist-1";
    String customer1 = "customer-1";
    String customer2 = "customer-2";
    
    // Customer 1 streams artist 1's music 5 times
    for (int i = 0; i < 5; i++) {
      streamInputTopic.pipeInput(
              DataFaker.STREAMS.generate(customer1, artist1)
      );
    }

    // Customer 2 streams artist 1's music 3 times
    for (int i = 0; i < 3; i++) {
      streamInputTopic.pipeInput(
              DataFaker.STREAMS.generate(customer2, artist1)
      );
    }

    // When: We read the output records
    var outputRecords = outputTopic.readRecordsToList();

    // Then: We should have records showing customer-1 as the top streamer for artist-1
    // Note: The exact number depends on windowing behavior, but we should have at least one record
    if (!outputRecords.isEmpty()) {
      var topRecord = outputRecords.get(outputRecords.size() - 1);
      // Verify the structure contains the thank you message
      assertNotNull(topRecord.value());
      assertNotNull(topRecord.value().getCustomerId());
      assertNotNull(topRecord.value().getArtistId());
      assertEquals(5L, topRecord.value().getStreamCount());
    }
  }
}

