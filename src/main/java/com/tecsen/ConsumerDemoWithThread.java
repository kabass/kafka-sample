package com.tecsen;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static Logger LOGGER = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public ConsumerDemoWithThread() {
    }

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }


    private void run(){

        // subscribe consumer to our iopic
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerRunnable  = new ConsumerRunnable(latch);
        Thread mythread  =  new Thread(myConsumerRunnable);
        mythread.start();
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("vaught shutdown hook");
            ((ConsumerRunnable)myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                LOGGER.error("Application has exited", e);
            } finally {

                LOGGER.error("Application is closing");
            }

        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Application got interrupted", e);
        } finally {

            LOGGER.error("Application is closing");
        }

    }

    public class ConsumerRunnable implements Runnable {


        public CountDownLatch latch;
        private Consumer<String, String> kafkaConsumer;
        public ConsumerRunnable(CountDownLatch latch){
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my seventh application");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            // create consumer
            kafkaConsumer = new KafkaConsumer<String, String>(properties);
            kafkaConsumer.subscribe(Arrays.asList("first_topic"));

        }
        @Override
        public void run() {
            try{
                while (true){
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100))    ;
                for(ConsumerRecord consumerRecord : records){
                    LOGGER.info(consumerRecord.toString());
                }
              }

            }  catch(WakeupException e) {
                LOGGER.info("Received shudown signal!");
            } finally  {
                kafkaConsumer.close();
                // tell our mai code we re done with consumer
                latch.countDown();
            }


        }

        public void shutdown(){
            kafkaConsumer.wakeup();
        }
    }
}
