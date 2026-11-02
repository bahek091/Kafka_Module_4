package com.example.kafka;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class DebeziumConsumer {

    private static final String TOPIC_USERS = "inventory.public.users";
    private static final String TOPIC_ORDERS = "inventory.public.orders";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "debezium-consumer-group-final");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {

            consumer.subscribe(Arrays.asList(TOPIC_USERS, TOPIC_ORDERS), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    System.out.println("[REBALANCE] Отозваны разделы: " + partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    System.out.println("[REBALANCE] Назначены разделы: " + partitions);
                }
            });

            System.out.println("Ожидание сообщений из Kafka...");

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(5));

                System.out.printf("Poll вернул записей: %d, назначенные разделы: %s%n",
                        records.count(), consumer.assignment());

                for (ConsumerRecord<String, byte[]> record : records) {
                    byte[] value = record.value();

                    System.out.printf(
                            "Топик: %s | Партиция: %d | Offset: %d | Ключ: %s%n",
                            record.topic(), record.partition(), record.offset(), record.key());

                    if (value == null) {
                        System.out.println("Значение: NULL (tombstone)");
                        continue;
                    }
                    System.out.println();
                }
            }
        }
    }
}