package ampw.store.read.KafkaConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreamsConsumer implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private final int id;

    private Logger logger = LoggerFactory.getLogger(KafkaStreamsConsumer.class);

    public KafkaStreamsConsumer(Properties consumerProperties, List<String> topics, int id) {
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.topics = topics;
        this.id = id;
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topics);
            logger.info("Consumer {} subscribed ...", id);

            while (true) {
                logger.info("Consumer {} polling ...", id);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofDays(1));
                logger.info("Received {} records", records.count());

                for (TopicPartition topicPartition : records.partitions()) {
                    List<ConsumerRecord<String, String>> topicRecords = records.records(topicPartition);

                    for (ConsumerRecord<String, String> record : topicRecords) {
                        logger.info("ConsumerId:{}-Topic:{} => Partition={}, Offset={}, EventTime:[{}] Val={}", id,
                                topicPartition.topic(), record.partition(), record.offset(), record.timestamp(),
                                record.value());
                    }

                    long lastPartitionOffset = topicRecords.get(topicRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(topicPartition,
                            new OffsetAndMetadata(lastPartitionOffset + 1)));
                }

            }
        } catch (WakeupException ignored) {
            // ignore for shutdown
        } catch (Exception e) {
            logger.error("Consumer encountered error", e);
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        //trigger a
        consumer.wakeup();
    }
}
