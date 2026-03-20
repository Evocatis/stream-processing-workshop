package org.improving.workshop.exercises.productiveArtists;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.msse.demo.mockdata.music.artist.Artist;
import org.msse.demo.mockdata.music.event.Event;

import static org.apache.kafka.streams.state.Stores.persistentKeyValueStore;
import static org.improving.workshop.Streams.*;

@Slf4j
public class ArtistRevenue {

    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-artist-revenue";

    public static void main(String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();
        configureTopology(builder);
        startStreams(builder);
    }

    public static void configureTopology(final StreamsBuilder builder) {

        // Events KTable — keyed by eventId
        var eventsTable = builder.table(
                TOPIC_DATA_DEMO_EVENTS,
                Materialized
                        .<String, Event>as(persistentKeyValueStore("productive-artist-events"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(SERDE_EVENT_JSON)
        );

        // Artists KTable — keyed by artistId
        var artistsTable = builder.table(
                TOPIC_DATA_DEMO_ARTISTS,
                Materialized
                        .<String, Artist>as(persistentKeyValueStore("productive-artist-details"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(SERDE_ARTIST_JSON)
        );

        builder
                .stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
                .peek((ticketId, ticket) -> log.info("Ticket received: {}", ticket))

                // 1. Rekey by eventId to join with eventsTable
                .selectKey((ticketId, ticket) -> ticket.eventid())

                // 2. Join ticket + event → extract (artistId, price)
                .join(
                        eventsTable,
                        (eventId, ticket, event) -> KeyValue.pair(event.artistid(), ticket.price().longValue())
                )

                // 3. Unwrap KeyValue into stream key/value
                .selectKey((eventId, pair) -> pair.key)
                .mapValues(pair -> pair.value)

                // 4. Sum revenue per artist using only primitive Serdes
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .reduce(
                        Long::sum,
                        Materialized
                                .<String, Long>as(persistentKeyValueStore("artist-revenue"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long())
                )

                .toStream()

                // 5. Join with artist details to enrich the record
                .join(
                        artistsTable,
                        (artistId, revenue, artist) -> new ArtistRevenueReport(artistId, artist, revenue)
                )

                .peek((artistId, report) -> log.info("Artist '{}' revenue: {}", artistId, report))

                .mapValues(ArtistRevenue::toJson)

                // 7. Collapse all artists to one key to track the global max
                .groupBy(
                        (artistId, reportJson) -> "global",
                        Grouped.with(Serdes.String(), Serdes.String())
                )
                .reduce(
                        // Keep whichever JSON string represents the higher revenue
                        (current, incoming) -> {
                            ArtistRevenueReport currentReport = fromJson(current);
                            ArtistRevenueReport incomingReport = fromJson(incoming);
                            return incomingReport.getRevenue() > currentReport.getRevenue() ? incoming : current;
                        },
                        Materialized
                                .<String, String>as(persistentKeyValueStore("max-revenue-artist"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String())
                )

                .toStream()
                .peek((k, json) -> {
                    ArtistRevenueReport report = fromJson(json);
                    log.info("Current max revenue artist: {} with ${}", report.getArtist().name(), report.getRevenue());
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ArtistRevenueReport {
        private String artistId;
        private Artist artist;
        private Long revenue;
    }


    private static String toJson(Object obj) {
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static ArtistRevenueReport fromJson(String json) {
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().readValue(json, ArtistRevenueReport.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}