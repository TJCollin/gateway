package cn.collin.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by collin on 17-5-10.
 */
public class SparkProducer extends Thread {
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final Boolean isAsync;
    private String messageStr;

    //0 represents the begin message, 1 represents the end message
    private Long messageKey;

    public SparkProducer(String topic, Boolean isAsync, String messageStr, Long messageKey){
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
        this.topic = topic;
        this.isAsync = isAsync;
        this.messageKey = messageKey;
        this.messageStr = messageStr;
    }

    public void run(){
        long startTime = System.currentTimeMillis();
        if (isAsync){
            producer.send(new ProducerRecord<String, String>(topic,messageKey + "",messageStr), new SparkProducer.DemoCallBack(startTime, messageStr, messageStr));
            producer.close();
        } else {
            try {
                producer.send(new ProducerRecord<String, String>(topic, messageKey + "", messageStr)).get();
                System.out.println("Sent message: (" + messageStr + ", " + messageStr + ")");
                producer.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    class DemoCallBack implements Callback {
        private final long startTime;
        private final String key;
        private final String message;

        public DemoCallBack(long startTime, String key, String message) {
            this.startTime = startTime;
            this.key = key;
            this.message = message;
        }

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            if (recordMetadata != null) {
                System.out.println("message(" + key + ", " + message + ") sent to partition(" + recordMetadata.partition() +
                        "), " +
                        "offset(" + recordMetadata.offset() + ") in " + elapsedTime + " ms");
            }
        }
    }
}
