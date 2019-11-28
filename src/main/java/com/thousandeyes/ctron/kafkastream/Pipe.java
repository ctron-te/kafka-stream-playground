package com.thousandeyes.ctron.kafkastream;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;



public class Pipe {


    public Topology buildTopology() {

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source =
                builder.stream("streams-plaintext-input");

        source.map((key, value) -> KeyValue.pair(key, value.toUpperCase()))
                .to("streams-pipe-output");

        Topology topology = builder.build();
        return topology;
    }
}
