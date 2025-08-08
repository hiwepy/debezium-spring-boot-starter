package io.debezium.client;

import io.debezium.client.pulsarmq.PulsarMQCanalConnector;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * PulsarMQ 模式 Canal 客户端
 */
@Slf4j
public class PulsarMQDebeziumClient extends AbstractMQDebeziumClient<PulsarMQCanalConnector> {

    private PulsarMQDebeziumClient(List<PulsarMQCanalConnector> connectors) {
        super(connectors);
    }

    @Override
    protected String getDestination(PulsarMQCanalConnector connector) {
        Field topicField =  ReflectionUtils.findField(PulsarMQCanalConnector.class, "topic");
        ReflectionUtils.makeAccessible(topicField);
        return (String) ReflectionUtils.getField(topicField, connector);
    }

    public static final class Builder extends AbstractClientBuilder<PulsarMQDebeziumClient, PulsarMQCanalConnector> {

        @Override
        public PulsarMQDebeziumClient build(List<PulsarMQCanalConnector> connectors) {
            PulsarMQDebeziumClient debeziumClient = new PulsarMQDebeziumClient(connectors);
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
