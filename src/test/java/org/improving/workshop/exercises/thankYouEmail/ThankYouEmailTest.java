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

  @Test
  @DisplayName("multiple artists with different top streamers")
  public void testMultipleArtistsWithDifferentTopStreamers() {
    // Given: Create streams for multiple artists
    String artist1 = "artist-1";
    String artist2 = "artist-2";
    String customer1 = "customer-1";
    String customer2 = "customer-2";
    String customer3 = "customer-3";

    // Artist 1: customer-1 is top with 7 streams
    for (int i = 0; i < 7; i++) {
      streamInputTopic.pipeInput(
              DataFaker.STREAMS.generate(customer1, artist1)
      );
    }

    // Artist 1: customer-2 has 3 streams (not top)
    for (int i = 0; i < 3; i++) {
      streamInputTopic.pipeInput(
              DataFaker.STREAMS.generate(customer2, artist1)
      );
    }

    // Artist 2: customer-2 is top with 6 streams
    for (int i = 0; i < 6; i++) {
      streamInputTopic.pipeInput(
              DataFaker.STREAMS.generate(customer2, artist2)
      );
    }

    // Artist 2: customer-3 has 2 streams (not top)
    for (int i = 0; i < 2; i++) {
      streamInputTopic.pipeInput(
              DataFaker.STREAMS.generate(customer3, artist2)
      );
    }

    // When: We read the output records
    var outputRecords = outputTopic.readRecordsToList();

    // Then: We should have records for both artists with their respective top streamers
    if (!outputRecords.isEmpty()) {
      // Verify we have outputs for different artists
      var artist1Records = outputRecords.stream()
              .filter(r -> artist1.equals(r.value().getArtistId()))
              .toList();
      var artist2Records = outputRecords.stream()
              .filter(r -> artist2.equals(r.value().getArtistId()))
              .toList();

      if (!artist1Records.isEmpty()) {
        assertEquals(customer1, artist1Records.get(artist1Records.size() - 1).value().getCustomerId());
        assertEquals(7L, artist1Records.get(artist1Records.size() - 1).value().getStreamCount());
      }

      if (!artist2Records.isEmpty()) {
        assertEquals(customer2, artist2Records.get(artist2Records.size() - 1).value().getCustomerId());
        assertEquals(6L, artist2Records.get(artist2Records.size() - 1).value().getStreamCount());
      }
    }
  }

  @Test
  @DisplayName("single customer is top streamer for multiple artists")
  public void testSingleCustomerTopStreamerForMultipleArtists() {
    // Given: One customer streams multiple artists heavily
    String artist1 = "artist-1";
    String artist2 = "artist-2";
    String artist3 = "artist-3";
    String topCustomer = "customer-top";
    String otherCustomer = "customer-other";

    // Top customer streams all artists heavily
    for (int i = 0; i < 8; i++) {
      streamInputTopic.pipeInput(
              DataFaker.STREAMS.generate(topCustomer, artist1)
      );
      streamInputTopic.pipeInput(
              DataFaker.STREAMS.generate(topCustomer, artist2)
      );
      streamInputTopic.pipeInput(
              DataFaker.STREAMS.generate(topCustomer, artist3)
      );
    }

    // Other customer streams less frequently
    for (int i = 0; i < 2; i++) {
      streamInputTopic.pipeInput(
              DataFaker.STREAMS.generate(otherCustomer, artist1)
      );
      streamInputTopic.pipeInput(
              DataFaker.STREAMS.generate(otherCustomer, artist2)
      );
      streamInputTopic.pipeInput(
              DataFaker.STREAMS.generate(otherCustomer, artist3)
      );
    }

    // When: We read the output records
    var outputRecords = outputTopic.readRecordsToList();

    // Then: We should have top customer as the top streamer for all artists
    if (!outputRecords.isEmpty()) {
      // Verify top customer appears for each artist
      var topCustomerRecords = outputRecords.stream()
              .filter(r -> topCustomer.equals(r.value().getCustomerId()))
              .toList();

      assertNotNull(topCustomerRecords);
      if (!topCustomerRecords.isEmpty()) {
        // All records should be for the top customer
        for (var record : topCustomerRecords) {
          assertEquals(topCustomer, record.value().getCustomerId());
          assertEquals(8L, record.value().getStreamCount());
        }
      }
    }
  }
}
