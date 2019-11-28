package com.thousandeyes.ctron.kafkastream;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;



public class PipeTest {

    private static final Logger log = LoggerFactory.getLogger(PipeTest.class);

    @Rule
    public KafkaContainer kafka = new KafkaContainer();

    @Test
    public void execute() throws Exception {
        String kafkaUrl = kafka.getBootstrapServers();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        AdminClient adminClient = AdminClient.create(props);

        log.info("Creating topic");

        CreateTopicsResult topics = adminClient.createTopics(
                List.of(new NewTopic("streams-plaintext-input", 1, (short) 1),
                        new NewTopic("streams-pipe-output", 1, (short) 1))
        );
        topics.all().get(5, TimeUnit.SECONDS);
        log.info("Topic created");

        Pipe pipe = new Pipe();
        Topology topology = pipe.buildTopology();

        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean continuePolling = new AtomicBoolean(true);

        ExecutorService threadPool = Executors.newFixedThreadPool(2);

        threadPool.submit(() -> {
            log.info("************** Starting producer thread");
            Properties producerProps = new Properties();
            producerProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
            producerProps.put("acks", "all");
            producerProps.put("key.serializer",
                              "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put("value.serializer",
                              "org.apache.kafka.common.serialization.StringSerializer");

            KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
            for (int i = 0; i < 20; i++) {
                log.info("Sending record " +i);
                producer.send(new ProducerRecord<String, String>("streams-plaintext-input",
                                                                 Integer.toString(i), "Foo" + i));
                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            producer.close();
            streams.close();
            latch.countDown();
        });

        threadPool.submit(() -> {
            log.info("************** Starting consumer thread");
            Properties consumerProps = new Properties();
            consumerProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
            consumerProps.setProperty("group.id", "test");
            consumerProps.setProperty("enable.auto.commit", "true");
            consumerProps.setProperty("auto.commit.interval.ms", "1000");
            consumerProps.setProperty("key.deserializer",
                                      "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.setProperty("value.deserializer",
                                      "org.apache.kafka.common.serialization.StringDeserializer");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(List.of("streams-pipe-output"));
            while (continuePolling.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> {
                    log.info(
                            String.format("****** Got [%s, %s]", record.key(), record.value()));
                });
            }
            consumer.close();
        });

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
                continuePolling.set(false);
            }
        });

        try {
            streams.start();
            latch.await();
        }
        catch (Throwable e) {
            e.printStackTrace();
        }
        System.out.println("All ok");
    }
}