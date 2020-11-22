package com.tecsen;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroupAssignAndSeek {
    public static Logger LOGGER = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static void main(String[] args) {



        // Create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // create consumer
        Consumer<String, String > consumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to our iopic

        TopicPartition parrtiotionToreadFromm = new TopicPartition("first_topic", 0);

        consumer.assign(Arrays.asList(parrtiotionToreadFromm));
        long offsetToReadFrom = 1L;

        consumer.seek(parrtiotionToreadFromm, offsetToReadFrom);

        int numberoMessageTorea = 5;
        boolean keepOnreading = true;
        int numberOfMessageReadSofar = 0;

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100))    ;
             for(ConsumerRecord consumerRecord : records){
                 LOGGER.info(consumerRecord.toString());
                 numberOfMessageReadSofar+=1;
                 if(numberOfMessageReadSofar >=numberoMessageTorea){
                     keepOnreading = false;
                     break;
                 }
             }
        }



    }
}
