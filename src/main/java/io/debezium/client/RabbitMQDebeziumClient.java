package io.debezium.client;

import io.debezium.client.rabbitmq.RabbitMQCanalConnector;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * RabbitMQ 模式 Canal 客户端
 */
public class RabbitMQDebeziumClient extends AbstractMQDebeziumClient<RabbitMQCanalConnector> {

    private RabbitMQDebeziumClient(List<RabbitMQCanalConnector> connectors) {
        super(connectors);
    }

    @Override
    protected String getDestination(RabbitMQCanalConnector connector) {
        Field nameServerField =  ReflectionUtils.findField(RabbitMQCanalConnector.class, "nameServer");
        ReflectionUtils.makeAccessible(nameServerField);
        return (String) ReflectionUtils.getField(nameServerField, connector);
    }

    public static final class Builder extends AbstractClientBuilder<RabbitMQDebeziumClient, RabbitMQCanalConnector> {

        @Override
        public RabbitMQDebeziumClient build(List<RabbitMQCanalConnector> connectors) {
            RabbitMQDebeziumClient debeziumClient = new RabbitMQDebeziumClient(connectors);
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
