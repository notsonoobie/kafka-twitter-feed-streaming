package com.kafka.twitterstream;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka - Twitter Tweet Streams
 */
public class KafkaTwitterStream {

    Logger logger = LoggerFactory.getLogger(KafkaTwitterStream.class.getName());

    /**
     * TWITTER API CONFIGS
    */
    final String TWITTER_APIKEY = "YOUR_TWITTER_API_KEY";
    final String TWITTER_APISECRETKEY = "YOUR_TWITTER_API_SECRET";
    final String TWITTER_ACCESSTOKEN = "YOUR_TWITTER_ACCESS_TOKEN";
    final String TWITTER_ACCESSSECRET = "YOUR_TWITTER_ACCESS_SECRET";

    /**
     * KAFKA SERVER CONFIGS
    */
    final String KAFKA_BOOTSTRAPSERVER = "127.0.0.1:9092";
    final String KAFKA_SERIALIZERNAME = StringSerializer.class.getName();
    final String KAFKA_TOPIC = "twitter_streams";

    public KafkaTwitterStream() {}

    public static void main(String[] args) {
        System.out.println(
            "\n" +
            "********************************************" + "\n" +
            "********************************************" + "\n" +
            "Starting Twitter Tweets Streaming with Kafka" + "\n" +
            "********************************************" + "\n" +
            "********************************************" + "\n" +
            "\n"
        );

        new KafkaTwitterStream().run();
    }

    public void run(){
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1);

        /**
         * 1. Create Twitter Client
         * 2. Create Kafka Producer
         * 3. Loop to send Tweets to Kafka
         */

        /**
         * Connect to Twitter Streams API
        */
        Client twitterClient = CreateTwitterClient(msgQueue);
        twitterClient.connect();

        /**
         * Create Kafka Producer
         */

        KafkaProducer<String, String> producer = CreateKafkaProducer();

        /**
         * ShutDown Hook
         */
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("****** Shutting Down TwitterClient" + "\n");
            twitterClient.stop();
            logger.info("****** Shutting Down Kafka Producer" + "\n");
            producer.close();
        }));

        while (!twitterClient.isDone()) {
            String msg;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);

                // Creating a record with a message, to be send on a topic.
                ProducerRecord<String, String> record = new ProducerRecord<String,String>(KAFKA_TOPIC, msg);

                // Producing Message to Kafka.
                producer.send(record, new Callback(){
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // Callback when data is sent to the Kafka Server
                        if(exception != null){
                            logger.error("Error while Producing Data", exception);
                        }else{
                            logger.info(
                                "\n" +
                                "=== MetaData === \n" +
                                "Topic           : " + metadata.topic() + "\n" +
                                "Topic Partition : " + metadata.partition() + "\n" +
                                "Offset          : " + metadata.offset() + "\n" +
                                "Timestamp       : " + metadata.timestamp() + "\n"
                            );
                        }
                    }
                });

                logger.info(msg + "\n");
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }
          }
    }

    /**
     * Create Twitter Client
     */
    public Client CreateTwitterClient(BlockingQueue<String> msgQueue){
        /**
         * Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        /**
         * TWITTER OAUTH1
         */
        Authentication hosebirdAuth = new OAuth1(
            TWITTER_APIKEY,
            TWITTER_APISECRETKEY,
            TWITTER_ACCESSTOKEN,
            TWITTER_ACCESSSECRET
        );

        ClientBuilder builder = new ClientBuilder()
            .name("Twitter-Streams-Kafka")
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    /**
     * Create Kafka Producer
    */
    public KafkaProducer<String, String> CreateKafkaProducer(){
        /**
         * Kafka Producer
        */

        // KAFKA PRODUCER PROPERTIES
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAPSERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZERNAME);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_SERIALIZERNAME);

        // Creating Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    }
}
