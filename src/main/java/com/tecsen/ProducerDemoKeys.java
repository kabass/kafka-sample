package com.tecsen;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static Logger LOGGER = LoggerFactory.getLogger(ProducerDemoKeys.class);
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        LOGGER.info("hello world");

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer =  new KafkaProducer<String, String>(properties);

        for(int i=0; i<10; i++) {
            String topic = "first_topic";
            String value = "Hello world ";
            String key = "id_"+i;
            // create record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value+i);
             LOGGER.info(key);

            // send data asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // execute everytime a record is successfully sent or note
                    if (e == null) {
                        // record is successfully send
                        LOGGER.info("received new metadat topic: {} \n Partition : {}\n offset : {}\n timestamp : {} ", recordMetadata.topic(),
                                recordMetadata.partition(),
                                recordMetadata.offset(),
                                recordMetadata.timestamp());
                    } else {
                        LOGGER.error("", e);
                    }

                }
            }).get(); // Block the send() to make i synchronous

        }
        // flush data
        producer.flush();

        //flush and close
        producer.close();

    }
}
