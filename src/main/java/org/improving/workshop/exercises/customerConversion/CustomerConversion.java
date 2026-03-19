package org.improving.workshop.exercises.customerConversion;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.msse.demo.mockdata.music.event.Event;
import org.msse.demo.mockdata.music.ticket.Ticket;
import org.springframework.kafka.support.serializer.JsonSerde;

import lombok.extern.slf4j.Slf4j;

import static org.improving.workshop.Streams.*;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class CustomerConversion {

    // custom serde
    public static final JsonSerde<TicketWithArtist> SERDE_TICKETWITHARTIST_JSON = new JsonSerde<>(TicketWithArtist.class);
    public static final JsonSerde<List<String>> SERDE_LIST_JSON = new JsonSerde<>(List.class);
    
    public static final String OUTPUT_TOPIC = "kafka-workshop-customer-conversion";

    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        configureTopology(builder);

        startStreams(builder);
    }

    static void configureTopology(final StreamsBuilder builder) {

        KTable<String, Event> eventsTable = builder
            .table(
                TOPIC_DATA_DEMO_EVENTS,
                Materialized
                    .<String, Event>as(Stores.persistentKeyValueStore("events-store"))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(SERDE_EVENT_JSON)
            );

        KTable<String, List<String>> listenedArtists = builder
            .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
            .selectKey((ignoredKey, streamRecord) -> streamRecord.customerid())
            .mapValues(streamRecord -> {
                List<String> list = new ArrayList<>();
                list.add(streamRecord.artistid());
                return list;
            })
            .groupByKey(Grouped.with(Serdes.String(), SERDE_LIST_JSON))
            .aggregate(
                ArrayList::new,
                (customer, newList, aggList) -> { 
                    for (String artist : newList) {
                        if (!aggList.contains(artist)) {
                            aggList.add(artist);
                        }
                    }
                    return aggList;
                },
            Materialized.with(Serdes.String(), SERDE_LIST_JSON)
    );

        
        KStream<String, TicketWithArtist> ticketArtistStream = builder
            .stream(TOPIC_DATA_DEMO_TICKETS, Consumed.with(Serdes.String(), SERDE_TICKET_JSON))
            .selectKey((ignoredKey, ticket) -> ticket.eventid())
            .join(
                eventsTable,
                (ticket, event) -> new TicketWithArtist(ticket, event.artistid()),
                Joined.with(Serdes.String(), SERDE_TICKET_JSON, SERDE_EVENT_JSON)
            )
            .selectKey((ignoredKey, ticket_w_artist) -> ticket_w_artist.ticket().customerid()
            );
        
        KTable<String, List<String>> purchasedArtists = ticketArtistStream
            .groupByKey(Grouped.with(Serdes.String(), SERDE_TICKETWITHARTIST_JSON))
            .aggregate(
                ArrayList::new,
                (customer, twa, aggList) -> {
                    String artistId = twa.artistId();
                    if (!aggList.contains(artistId)) {
                        aggList.add(artistId);
                    }
                    return aggList;
                },
                Materialized.with(Serdes.String(), SERDE_LIST_JSON)
            );

        KTable<String, Boolean> customerMatch = listenedArtists.outerJoin(
            purchasedArtists,
            (listened, purchased) -> {
                if (listened == null || purchased == null) {
                    return false;
                }
                for (String artist : listened) {
                    if (purchased.contains(artist)) {
                        return true;
                    }
                }
                return false;
            }
            );
        
        customerMatch
            .toStream()
            .filter((customer, match) -> match)
            .groupBy((customer, match) -> "TALLY", Grouped.with(Serdes.String(), Serdes.Boolean()))
            .count(Materialized.as("listened-ticket-tally-store"))
            .toStream()
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }

    public static record TicketWithArtist(Ticket ticket, String artistId) { }

}
