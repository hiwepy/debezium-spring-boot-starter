package io.debezium.embedded.client;

import io.debezium.client.kafka.KafkaCanalConnector;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Kafka 模式 Canal 客户端
 */
@Slf4j
public class KafkaDebeziumClient extends AbstractMQDebeziumClient<KafkaCanalConnector> {

    private KafkaDebeziumClient(List<KafkaCanalConnector> connectors) {
        super(connectors);
    }

    @Override
    protected String getDestination(KafkaCanalConnector connector) {
        Field topicField =  ReflectionUtils.findField(KafkaCanalConnector.class, "topic");
        ReflectionUtils.makeAccessible(topicField);
        return (String) ReflectionUtils.getField(topicField, connector);
    }

    public static final class Builder extends AbstractClientBuilder<KafkaDebeziumClient, KafkaCanalConnector> {

        @Override
        public KafkaDebeziumClient build(List<KafkaCanalConnector> connectors) {
            KafkaDebeziumClient debeziumClient = new KafkaDebeziumClient(connectors);
            debeziumClient.setBatchSize(batchSize);
            debeziumClient.setFilter(filter);
            debeziumClient.setMessageHandler(messageHandler);
            debeziumClient.setTimeout(timeout);
            debeziumClient.setUnit(unit);
            debeziumClient.setSubscribeTypes(subscribeTypes);
            return debeziumClient;
        }
    }

}
