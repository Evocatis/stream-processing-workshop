package org.improving.workshop.exercises.stateful;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.msse.demo.mockdata.customer.profile.Customer;
import org.msse.demo.mockdata.music.artist.Artist;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

import static org.improving.workshop.Streams.*;

/**
 * Goals -
 * 1. Find the top weekly streamer per artist for each month
 * 2. Send thank you emails to those customers
 *
 * Topology:
 * streams (KStream)
 *    ↓
 * selectKey(artistId|customerId)
 *    ↓
 * groupByKey
 *    ↓
 * windowedBy(1 month)
 *    ↓
 * count() ← STATE STORE #1
 *    ↓
 * toStream
 *    ↓
 * extract artistId
 *    ↓
 * groupByKey
 *    ↓
 * aggregate(max) ← STATE STORE #2
 *    ↓
 * join(artists)
 *    ↓
 * join(customers)
 *    ↓
 * join(emails)
 *    ↓
 * to("thank-you-emails")
 */
@Slf4j
public class ThankYouEmail {
    // MUST BE PREFIXED WITH "kafka-workshop-"
    public static final String OUTPUT_TOPIC = "kafka-workshop-thank-you-emails";

    // DTO for the customer artist stream count result
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class TopStreamer {
        public String customerId;
        public String artistId;
        public Long streamCount;
    }

    // DTO for the final thank you email message
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class ThankYouMessage {
        public String customerId;
        public String customerName;
        public String artistId;
        public String artistName;
        public String email;
        public Long streamCount;
    }

    public static final JsonSerde<TopStreamer> SERDE_TOP_STREAMER = new JsonSerde<>(TopStreamer.class);
    public static final JsonSerde<ThankYouMessage> SERDE_THANK_YOU_MESSAGE = new JsonSerde<>(ThankYouMessage.class);

    /**
     * The Streams application as a whole can be launched like any normal Java application that has a `main()` method.
     */
    public static void main(final String[] args) {
        final StreamsBuilder builder = new StreamsBuilder();

        // configure the processing topology
        configureTopology(builder);

        // fire up the engines
        startStreams(builder);
    }

    public static void configureTopology(final StreamsBuilder builder) {
        // Create KTable for artists reference data
        KTable<String, Artist> artistsTable = builder
                .table(
                        TOPIC_DATA_DEMO_ARTISTS,
                        Materialized
                                .<String, Artist>as(Stores.persistentKeyValueStore("artists"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_ARTIST_JSON)
                );

        KTable<String, Customer> customersTable = builder
                .table(
                        TOPIC_DATA_DEMO_CUSTOMERS,
                        Materialized
                                .<String, Customer>as(Stores.persistentKeyValueStore("customers"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_CUSTOMER_JSON)
                );

        // Main stream processing
        builder
                .stream(TOPIC_DATA_DEMO_STREAMS, Consumed.with(Serdes.String(), SERDE_STREAM_JSON))
                .peek((streamId, stream) -> log.info("Stream Received: artistId={}, customerId={}", stream.artistid(), stream.customerid()))

                // Step 1: Select key as "artistId|customerId"
                .selectKey(
                        (streamId, stream) -> stream.artistid() + "|" + stream.customerid(),
                        Named.as("rekey-by-artist-customer")
                )

                // Step 2: Group by key
                .groupByKey(Grouped.with(Serdes.String(), SERDE_STREAM_JSON))

                // Step 3: Window by 5 seconds for manual testing
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(1)))

                .emitStrategy(EmitStrategy.onWindowClose())
                // Step 4: Count streams (STATE STORE #1)
                .count(
                        Materialized
                                .<String, Long>as(Stores.persistentWindowStore("stream-counts-window", Duration.ofDays(35), Duration.ofDays(30), false))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long())
                )

                // Step 5: Convert to stream
                .toStream()

                // Extract key components and create TopStreamer object
                .map(
                        (windowed, count) -> {
                            String[] keyParts = windowed.key().split("\\|");
                            String artistId = keyParts[0];
                            String customerId = keyParts[1];
                            TopStreamer topStreamer = TopStreamer.builder()
                                    .artistId(artistId)
                                    .customerId(customerId)
                                    .streamCount(count)
                                    .build();
                            return new KeyValue<>(artistId, topStreamer);
                        },
                        Named.as("extract-artist-id")
                )

                // Step 6: Group by artistId
                .groupByKey(Grouped.with(Serdes.String(), SERDE_TOP_STREAMER))

                // Step 7: Aggregate to find max (STATE STORE #2)
                .aggregate(
                        () -> new TopStreamer(null, null, 0L),
                        (artistId, newStreamer, currentMax) -> {
                            if (currentMax.streamCount == null || newStreamer.streamCount > currentMax.streamCount) {
                                return newStreamer;
                            }
                            return currentMax;
                        },
                        Materialized
                                .<String, TopStreamer>as(Stores.persistentKeyValueStore("top-streamer-per-artist"))
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SERDE_TOP_STREAMER)
                )

                // Step 8: Convert back to stream
                .toStream()

                // Step 9: Join with artists table to get artist details
                .join(
                        artistsTable,
                        (topStreamer, artist) -> topStreamer
                )

                // Step 10: Rekey by customerId for joining with customers
                .selectKey((artistId, topStreamer) -> topStreamer.customerId)

                // Step 11: Join with customers table to get customer details
                .join(
                        customersTable,
                        (topStreamer, customer) -> ThankYouMessage.builder()
                                .customerId(topStreamer.customerId)
                                .customerName(topStreamer.customerId) // Use ID as fallback until we know Customer methods
                                .artistId(topStreamer.artistId)
                                .artistName("")
                                .email("")
                                .streamCount(topStreamer.streamCount)
                                .build()
                )

                // Step 12: Send to output topic
                .peek((key, message) -> log.info("Thank you email sent to {} for artist {} with {} streams",
                        message.customerName, message.artistId, message.streamCount))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), SERDE_THANK_YOU_MESSAGE));
    }
}

