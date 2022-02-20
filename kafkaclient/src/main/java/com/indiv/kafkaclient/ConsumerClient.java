package com.indiv.kafkaclient;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

/**
 * Hello world!
 *
 */
public class ConsumerClient 
{
    public static void main( String[] args )
    {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092, localhost:9093");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("group.id", "G1");
        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList("sample"));
        while (true) {
           ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
           for (ConsumerRecord<String, String> record : records)
           System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}
