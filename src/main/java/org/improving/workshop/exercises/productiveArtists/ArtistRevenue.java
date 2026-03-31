package org.improving.workshop.exercises.productiveArtists;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
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

    public static final String OUTPUT_TOPIC = "kafka-workshop-artist-revenue";

    public static void main(String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();
        configureTopology(builder);
        startStreams(builder);
    }

    public static void configureTopology(final StreamsBuilder builder) {

        var eventsGlobalTable = builder.globalTable(
                TOPIC_DATA_DEMO_EVENTS,
                Materialized
                        .<String, Event>as(persistentKeyValueStore("productive-artist-events"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(SERDE_EVENT_JSON)
        );

        var artistsGlobalTable = builder.globalTable(
                TOPIC_DATA_DEMO_ARTISTS,
                Materialized
                        .<String, Artist>as(persistentKeyValueStore("productive-artist-details"))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(SERDE_ARTIST_JSON)
        );

        builder
                .stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
                .peek((ticketId, ticket) -> log.info("Ticket received: {}", ticket))

                .join(
                        eventsGlobalTable,
                        (ticketId, ticket) -> ticket.eventid(),
                        (ticket, event) -> new TicketEventPair(event.artistid(), ticket.price().longValue())
                )

                .selectKey((ticketId, pair) -> pair.getArtistId())
                .mapValues(TicketEventPair::getRevenue)

                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                .reduce(
                        Long::sum,
                        Materialized
                                .<String, Long>as(persistentKeyValueStore("artist-revenue"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long())
                )

                .toStream()

                .join(
                        artistsGlobalTable,
                        (artistId, revenue) -> artistId,
                        (revenue, artist) -> new ArtistRevenueReport(artist.id(), artist, revenue)
                )

                // .peek((artistId, report) -> log.info("Enriched report: {}", report))

                .mapValues(ArtistRevenue::toJson)

                .groupBy(
                        (artistId, reportJson) -> "global",
                        Grouped.with(Serdes.String(), Serdes.String())
                )
                .reduce(
                        (current, incoming) -> {
                            ArtistRevenueReport cur = fromJson(current);
                            ArtistRevenueReport inc = fromJson(incoming);
                            return inc.getRevenue() > cur.getRevenue() ? incoming : current;
                        },
                        Materialized
                                .<String, String>as(persistentKeyValueStore("max-revenue-artist"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String())
                )

                .toStream()
                // .peek((k, json) -> {
                //     ArtistRevenueReport report = fromJson(json);
                //     log.info("Current max revenue artist: {} with ${}", report.getArtist().name(), report.getRevenue());
                // })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TicketEventPair {
        private String artistId;
        private Long revenue;
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