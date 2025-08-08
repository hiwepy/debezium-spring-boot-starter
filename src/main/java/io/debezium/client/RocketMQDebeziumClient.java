package io.debezium.client;

import io.debezium.client.rocketmq.RocketMQCanalConnector;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * RocketMQ 模式 Canal 客户端
 */
public class RocketMQDebeziumClient extends AbstractMQDebeziumClient<RocketMQCanalConnector> {

    public RocketMQDebeziumClient(List<RocketMQCanalConnector> connectors) {
        super(connectors);
    }

    @Override
    protected String getDestination(RocketMQCanalConnector connector) {
        Field topicField =  ReflectionUtils.findField(RocketMQCanalConnector.class, "topic");
        ReflectionUtils.makeAccessible(topicField);
        return (String) ReflectionUtils.getField(topicField, connector);
    }

    public static final class Builder extends AbstractClientBuilder<RocketMQDebeziumClient, RocketMQCanalConnector> {

        @Override
        public RocketMQDebeziumClient build(List<RocketMQCanalConnector> connectors) {
            RocketMQDebeziumClient debeziumClient = new RocketMQDebeziumClient(connectors);
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
