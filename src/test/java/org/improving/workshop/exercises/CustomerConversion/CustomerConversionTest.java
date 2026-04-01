package org.improving.workshop.exercises.CustomerConversion;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.improving.workshop.Streams;
import org.improving.workshop.exercises.customerConversion.CustomerConversion;
import org.junit.jupiter.api.*;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.stream.Stream;
import org.msse.demo.mockdata.music.ticket.Ticket;

import java.util.List;
import java.util.UUID;

import static org.improving.workshop.utils.DataFaker.STREAMS;
import static org.improving.workshop.utils.DataFaker.TICKETS;
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
    String artist   = "artist-1";
    String customer = "customer-1";
    String eventId  = "event-1";

    // Given: an event exists for artist-1
    eventsInputTopic.pipeInput(eventId, new Event(eventId, artist, "venue-1", 100, "2025-01-01"));

    // And: customer-1 has streamed artist-1
    streamsInputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate(customer, artist));

    // And: customer-1 bought a ticket to artist-1's event
    ticketsInputTopic.pipeInput("ticket-1", TICKETS.generate(customer, eventId));

    // When
    List<TestRecord<String, Long>> results = outputTopic.readRecordsToList();

    // Then: one converted customer is tallied
    assertFalse(results.isEmpty());
    TestRecord<String, Long> last = results.get(results.size() - 1);
    assertEquals("TALLY", last.key());
    assertEquals(1L, last.value());
  }

  // ---------------------------------------------------------
  // Test 2: No Match — streamed artist differs from purchased artist
  // ---------------------------------------------------------
  @Test
  @DisplayName("customer not counted when listened and purchased artists differ")
  public void testNoMatch() {
    String customer = "customer-1";
    String eventId  = "event-1";

    // Given: an event exists for artist-B
    eventsInputTopic.pipeInput(eventId, new Event(eventId, "artist-B", "venue-1", 100, "2025-01-01"));

    // And: customer-1 has only ever streamed artist-A
    streamsInputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate(customer, "artist-A"));

    // And: customer-1 buys a ticket to artist-B's event (no match)
    ticketsInputTopic.pipeInput("ticket-1", TICKETS.generate(customer, eventId));

    // When
    List<TestRecord<String, Long>> results = outputTopic.readRecordsToList();

    // Then: no conversion — the streamed artist never matches the purchased artist
    assertTrue(results.isEmpty());
  }

  // ---------------------------------------------------------
  // Test 3: Deduplication Behavior
  // ---------------------------------------------------------
  @Test
  @DisplayName("duplicate artist listens are deduplicated; tally increments once per matching ticket")
  public void testDeduplication() {
    String artist   = "artist-1";
    String customer = "customer-1";
    String eventId  = "event-1";

    // Given: an event exists for artist-1
    eventsInputTopic.pipeInput(eventId, new Event(eventId, artist, "venue-1", 100, "2025-01-01"));

    // And: customer-1 streams artist-1 three times — the topology deduplicates these
    // into a single entry in the listenedArtists KTable
    for (int i = 0; i < 3; i++) {
      streamsInputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate(customer, artist));
    }

    // And: customer-1 purchases 2 tickets — each ticket triggers a new KTable join evaluation
    for (int i = 0; i < 2; i++) {
      ticketsInputTopic.pipeInput("ticket-" + i, TICKETS.generate(customer, eventId));
    }

    // When
    List<TestRecord<String, Long>> results = outputTopic.readRecordsToList();

    assertFalse(results.isEmpty());
    TestRecord<String, Long> last = results.get(results.size() - 1);
    assertEquals(2L, last.value());
  }

  // ---------------------------------------------------------
  // Test 4: Multiple customers — only matching ones are counted
  // ---------------------------------------------------------
  @Test
  @DisplayName("only customers who streamed the purchased artist are counted")
  public void testMultipleCustomers() {
    String artist  = "artist-1";
    String eventId = "event-1";

    // Given: an event for artist-1
    eventsInputTopic.pipeInput(eventId, new Event(eventId, artist, "venue-1", 200, "2025-06-01"));

    // And: customer-1 streamed artist-1 (will match)
    streamsInputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate("customer-1", artist));

    // And: customer-2 streamed a different artist (will not match)
    streamsInputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate("customer-2", "artist-other"));

    // And: customer-3 streamed artist-1 (will match)
    streamsInputTopic.pipeInput(UUID.randomUUID().toString(), STREAMS.generate("customer-3", artist));

    // And: all three customers buy tickets to artist-1's event
    ticketsInputTopic.pipeInput("ticket-1", TICKETS.generate("customer-1", eventId));
    ticketsInputTopic.pipeInput("ticket-2", TICKETS.generate("customer-2", eventId));
    ticketsInputTopic.pipeInput("ticket-3", TICKETS.generate("customer-3", eventId));

    // When
    List<TestRecord<String, Long>> results = outputTopic.readRecordsToList();

    // Then: only customer-1 and customer-3 convert — tally should be 2
    assertFalse(results.isEmpty());
    TestRecord<String, Long> last = results.get(results.size() - 1);
    assertEquals("TALLY", last.key());
    assertEquals(2L, last.value());
  }
}