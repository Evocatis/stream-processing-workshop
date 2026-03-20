package org.improving.workshop.exercises.customerConversion;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.improving.workshop.Streams;
import org.junit.jupiter.api.*;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.stream.Stream;
import org.msse.demo.mockdata.music.ticket.Ticket;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CustomerConversionTest {

  private TopologyTestDriver driver;

  private TestInputTopic<String, Event> eventsInputTopic;
  private TestInputTopic<String, Stream> streamsInputTopic;
  private TestInputTopic<String, Ticket> ticketsInputTopic;

  private TestOutputTopic<String, Long> outputTopic;

  @BeforeEach
  public void setup() {

    StreamsBuilder streamsBuilder = new StreamsBuilder();
    CustomerConversion.configureTopology(streamsBuilder);

    driver = new TopologyTestDriver(streamsBuilder.build(), Streams.buildProperties());

    eventsInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_EVENTS,
            Serdes.String().serializer(),
            Streams.SERDE_EVENT_JSON.serializer()
    );

    streamsInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_STREAMS,
            Serdes.String().serializer(),
            Streams.SERDE_STREAM_JSON.serializer()
    );

    ticketsInputTopic = driver.createInputTopic(
            Streams.TOPIC_DATA_DEMO_TICKETS,
            Serdes.String().serializer(),
            Streams.SERDE_TICKET_JSON.serializer()
    );

    outputTopic = driver.createOutputTopic(
            CustomerConversion.OUTPUT_TOPIC,
            Serdes.String().deserializer(),
            Serdes.Long().deserializer()
    );
  }

  @AfterEach
  public void cleanup() {
    driver.close();
  }

  // ---------------------------------------------------------
  // Test 1: Matching Customer Conversion
  // ---------------------------------------------------------
  @Test
  @DisplayName("customer counted when listened artist matches purchased artist")
  public void testCustomerMatch() {

    String artist = "artist-1";
    String customer = "customer-1";
    String eventId = "event-1";

    // Given
    eventsInputTopic.pipeInput(eventId, new Event(eventId, artist));

    streamsInputTopic.pipeInput(
            null,
            new Stream(customer, artist)
    );

    ticketsInputTopic.pipeInput(
            "ticket-1",
            new Ticket("ticket-1", eventId, customer)
    );

    // When
    List<TestRecord<String, Long>> results = outputTopic.readRecordsToList();

    // Then
    assertFalse(results.isEmpty());

    TestRecord<String, Long> last = results.get(results.size() - 1);

    assertEquals("TALLY", last.key());
    assertEquals(1L, last.value());
  }

  // ---------------------------------------------------------
  // Test 2: No Match Scenario
  // ---------------------------------------------------------
  @Test
  @DisplayName("customer not counted when listened and purchased artists differ")
  public void testNoMatch() {

    String customer = "customer-1";

    // Given
    eventsInputTopic.pipeInput("event-1", new Event("event-1", "artist-B"));

    streamsInputTopic.pipeInput(
            null,
            new Stream(customer, "artist-A")
    );

    ticketsInputTopic.pipeInput(
            "ticket-1",
            new Ticket("ticket-1", "event-1", customer)
    );

    // When
    List<TestRecord<String, Long>> results = outputTopic.readRecordsToList();

    // Then
    assertTrue(results.isEmpty());
  }

  // ---------------------------------------------------------
  // Test 3: Deduplication Behavior
  // ---------------------------------------------------------
  @Test
  @DisplayName("duplicate listens and purchases do not inflate count")
  public void testDeduplication() {

    String artist = "artist-1";
    String customer = "customer-1";
    String eventId = "event-1";

    // Given
    eventsInputTopic.pipeInput(eventId, new Event(eventId, artist));

    // Duplicate listens
    for (int i = 0; i < 3; i++) {
      streamsInputTopic.pipeInput(
              null,
              new Stream(customer, artist)
      );
    }

    // Duplicate purchases
    for (int i = 0; i < 2; i++) {
      ticketsInputTopic.pipeInput(
              "ticket-" + i,
              new Ticket("ticket-" + i, eventId, customer)
      );
    }

    // When
    List<TestRecord<String, Long>> results = outputTopic.readRecordsToList();

    // Then
    assertFalse(results.isEmpty());

    TestRecord<String, Long> last = results.get(results.size() - 1);

    assertEquals(1L, last.value());
  }
}
