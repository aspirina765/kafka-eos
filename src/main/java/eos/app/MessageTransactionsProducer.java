package eos.app;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class MessageTransactionsProducer {
  public static void main(String[] args) {
    Properties properties = new Properties();

    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    Producer<String, String> producer = new KafkaProducer<>(properties);

    int i = 0;
    while (true) {
      System.out.println("Produzindo batch: " + i);
      try {
        producer.send(newRandomMessageTransaction("message 1"));
        Thread.sleep(0);
        producer.send(newRandomMessageTransaction("message 2"));
        Thread.sleep(0);
        producer.send(newRandomMessageTransaction("message 3"));
        Thread.sleep(0);
        i += 1;
      } catch (InterruptedException e) {
        break;
      }
    }
    producer.close();

  }

  private static ProducerRecord<String, String> newRandomMessageTransaction(String messageBody) {

    ObjectNode messageTransaction = JsonNodeFactory.instance.objectNode();


    Integer idMessageTransaction = ThreadLocalRandom.current().nextInt(0, 1000);

    Instant now = Instant.now();

    messageTransaction.put("messageBody", messageBody);
    messageTransaction.put("idMessageTransaction", idMessageTransaction);
    messageTransaction.put("time", now.toString());
    return new ProducerRecord<>("message-transactions", messageBody, messageTransaction.toString());


  }


}
