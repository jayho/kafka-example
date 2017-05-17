package com.rwe.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;
import java.util.stream.IntStream;

public class Main {
  private static final String TOPIC = "test";

  public static void main(String[] args) throws IOException {
    Producer<String, String> producer = new KafkaProducer<>(getProperties());

    IntStream.range(0, 100).forEach(i ->
      producer.send(new ProducerRecord<>(TOPIC, "k" + Integer.toString(i), "v" + Integer.toString(i)),
        (metadata, exception) -> System.out.println("Sent: " + i)));

    producer.close();
  }

  private static Properties getProperties() throws IOException {
    // Load properties
    Properties props = new Properties();
    props.load(ClassLoader.getSystemResourceAsStream("config.properties"));
    return props;
  }

}
