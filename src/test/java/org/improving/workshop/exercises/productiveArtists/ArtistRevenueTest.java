package org.improving.workshop.exercises.productiveArtists;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.improving.workshop.Streams;
import org.junit.jupiter.api.*;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class ArtistRevenueTest {

    private TopologyTestDriver driver;
    private final ObjectMapper mapper = new ObjectMapper();

    private TestInputTopic<String, Event>   eventTopic;
    private TestInputTopic<String, Artist>  artistTopic;
    private TestInputTopic<String, Ticket>  ticketTopic;

    private TestOutputTopic<String, String> outputTopic;

    @BeforeEach
    void setup() {
        StreamsBuilder builder = new StreamsBuilder();
        ArtistRevenue.configureTopology(builder);

        driver = new TopologyTestDriver(builder.build(), Streams.buildProperties());

        eventTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_EVENTS,
                Serdes.String().serializer(),
                Streams.SERDE_EVENT_JSON.serializer()
        );

        artistTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_ARTISTS,
                Serdes.String().serializer(),
                Streams.SERDE_ARTIST_JSON.serializer()
        );

        ticketTopic = driver.createInputTopic(
                Streams.TOPIC_DATA_DEMO_TICKETS,
                Serdes.String().serializer(),
                Streams.SERDE_TICKET_JSON.serializer()
        );

        outputTopic = driver.createOutputTopic(
                ArtistRevenue.OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                Serdes.String().deserializer()
        );
    }

    @AfterEach
    void cleanup() {
        driver.close();
    }

    private ArtistRevenue.ArtistRevenueReport parse(String json) {
        try {
            return mapper.readValue(json, ArtistRevenue.ArtistRevenueReport.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse: " + json, e);
        }
    }

    @Test
    @DisplayName("identifies the artist with maximum revenue correctly")
    void testMaxRevenueArtist() {
        artistTopic.pipeInput("artistA", new Artist("artistA", "Taylor Swift", "pop"));
        artistTopic.pipeInput("artistB", new Artist("artistB", "Drake", "hip-hop"));
        artistTopic.pipeInput("artistC", new Artist("artistC", "Adele", "soul"));

        eventTopic.pipeInput("event1", new Event("event1", "artistA", "venue1", 100, "2024-01-01"));
        eventTopic.pipeInput("event2", new Event("event2", "artistB", "venue2", 100, "2024-01-02"));
        eventTopic.pipeInput("event3", new Event("event3", "artistC", "venue3", 100, "2024-01-03"));

        ticketTopic.pipeInput(UUID.randomUUID().toString(), new Ticket("t1", "cust1", "event1", 100.0));
        ticketTopic.pipeInput(UUID.randomUUID().toString(), new Ticket("t2", "cust2", "event2", 500.0));
        ticketTopic.pipeInput(UUID.randomUUID().toString(), new Ticket("t3", "cust3", "event3", 300.0));

        List<TestRecord<String, String>> results = outputTopic.readRecordsToList();

        var lastGlobal = results.stream()
                .filter(r -> r.key().equals("global"))
                .reduce((first, second) -> second)
                .orElseThrow();

        assertEquals("Drake", parse(lastGlobal.value()).getArtist().name());
        assertEquals(500L,    parse(lastGlobal.value()).getRevenue());
    }

    @Test
    @DisplayName("max revenue updates as new tickets arrive — leadership changes over time")
    void testMaxRevenueUpdatesOverTime() {
        artistTopic.pipeInput("artistA", new Artist("artistA", "Taylor Swift", "pop"));
        artistTopic.pipeInput("artistB", new Artist("artistB", "Drake", "hip-hop"));

        eventTopic.pipeInput("event1", new Event("event1", "artistA", "venue1", 100, "2024-01-01"));
        eventTopic.pipeInput("event2", new Event("event2", "artistB", "venue2", 100, "2024-01-02"));

        ticketTopic.pipeInput(UUID.randomUUID().toString(), new Ticket("t1", "cust1", "event1", 300.0));
        ticketTopic.pipeInput(UUID.randomUUID().toString(), new Ticket("t2", "cust2", "event2", 200.0));
        ticketTopic.pipeInput(UUID.randomUUID().toString(), new Ticket("t3", "cust3", "event2", 200.0));

        List<TestRecord<String, String>> results = outputTopic.readRecordsToList();
        List<String> maxArtistOverTime = results.stream()
                .filter(r -> r.key().equals("global"))
                .map(r -> parse(r.value()).getArtist().name())
                .toList();

        assertEquals("Taylor Swift", maxArtistOverTime.get(0));
        assertEquals("Taylor Swift", maxArtistOverTime.get(1));
        assertEquals("Drake",        maxArtistOverTime.get(2));
    }

    @Test
    @DisplayName("single ticket produces a single max revenue record")
    void testSingleTicket() {
        artistTopic.pipeInput("artistA", new Artist("artistA", "Taylor Swift", "pop"));
        eventTopic.pipeInput("event1", new Event("event1", "artistA", "venue1", 100, "2024-01-01"));

        ticketTopic.pipeInput(UUID.randomUUID().toString(), new Ticket("t1", "cust1", "event1", 75.0));

        List<TestRecord<String, String>> results = outputTopic.readRecordsToList();

        assertEquals(1, results.size());
        assertEquals("global",       results.get(0).key());
        assertEquals(75L,            parse(results.get(0).value()).getRevenue());
        assertEquals("Taylor Swift", parse(results.get(0).value()).getArtist().name());
    }

    @Test
    @DisplayName("ticket for unknown event is dropped — no output emitted")
    void testTicketForUnknownEventIsDropped() {
        artistTopic.pipeInput("artistA", new Artist("artistA", "Taylor Swift", "pop"));

        ticketTopic.pipeInput(UUID.randomUUID().toString(), new Ticket("t1", "cust1", "event-unknown", 50.0));

        assertTrue(outputTopic.isEmpty());
    }

    @Test
    @DisplayName("cumulative revenue across multiple tickets for same artist")
    void testCumulativeRevenueForSameArtist() {
        artistTopic.pipeInput("artistA", new Artist("artistA", "Taylor Swift", "pop"));
        eventTopic.pipeInput("event1", new Event("event1", "artistA", "venue1", 100, "2024-01-01"));

        ticketTopic.pipeInput(UUID.randomUUID().toString(), new Ticket("t1", "cust1", "event1", 50.0));
        ticketTopic.pipeInput(UUID.randomUUID().toString(), new Ticket("t2", "cust2", "event1", 50.0));
        ticketTopic.pipeInput(UUID.randomUUID().toString(), new Ticket("t3", "cust3", "event1", 50.0));
        ticketTopic.pipeInput(UUID.randomUUID().toString(), new Ticket("t4", "cust4", "event1", 50.0));

        List<TestRecord<String, String>> results = outputTopic.readRecordsToList();

        var last = results.stream()
                .reduce((first, second) -> second)
                .orElseThrow();

        assertEquals(200L,           parse(last.value()).getRevenue());
        assertEquals("Taylor Swift", parse(last.value()).getArtist().name());
    }
}