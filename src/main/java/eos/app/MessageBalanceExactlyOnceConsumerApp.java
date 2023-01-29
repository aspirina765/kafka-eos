package eos.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.time.Instant;
import java.util.Properties;

public class MessageBalanceExactlyOnceConsumerApp {
  public static void main(String[] args) {
    Properties config = new Properties();

    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "message-balance-application");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

    final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
    final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
    final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

    KStreamBuilder builder = new KStreamBuilder();

    KStream<String, JsonNode> messageTransactions =
      builder.stream(Serdes.String(), jsonSerde, "message-transactions");


    ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
    initialBalance.put("count", 0);
    initialBalance.put("balance", 0);
    initialBalance.put("time", Instant.ofEpochMilli(0L).toString());

    KStream<String, JsonNode> messageBalance = messageTransactions
    .groupByKey(Serdes.String(), jsonSerde)
      .aggregate(
        () -> initialBalance,
        (key, messageTransaction, balance) -> (JsonNode) newBalance(messageTransaction, balance),
        jsonSerde,
        "message-balance-agg"
      ).toStream();

    messageBalance.to(Serdes.String(), jsonSerde, "message-balance-exactly-once");

    KafkaStreams streams = new KafkaStreams(builder, config);
    streams.cleanUp();
    streams.start();

    System.out.println(streams);


    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

  }

  private static Object newBalance(JsonNode messageTransaction, JsonNode balance) {

    ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
    newBalance.put("count", balance.get("count").asInt() + 1);
    newBalance.put("balance", balance.get("balance").asInt() + messageTransaction.get("idMessageTransaction").asInt());

    Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
    Long transactionEpoch = Instant.parse(messageTransaction.get("time").asText()).toEpochMilli();
    Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
    newBalance.put("time", newBalanceInstant.toString());
    return newBalance;
  }
}
