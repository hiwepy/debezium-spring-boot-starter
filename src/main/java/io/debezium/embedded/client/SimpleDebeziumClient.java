package io.debezium.embedded.client;

import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Simple 模式 Debezium 客户端
 */
public class SimpleDebeziumClient extends AbstractDebeziumClient<SimpleCanalConnector> {

    private SimpleDebeziumClient(List<SimpleCanalConnector> connectors) {
        super(connectors);
    }

    @Override
    protected String getDestination(SimpleCanalConnector connector) { // Changed from SimpleCanalConnector to SimpleCanalConnector
        Field clientIdentityField = ReflectionUtils.findField(SimpleCanalConnector.class, "clientIdentity");
        ReflectionUtils.makeAccessible(clientIdentityField);
        ClientIdentity clientIdentity = (ClientIdentity) ReflectionUtils.getField(clientIdentityField, connector);
        return clientIdentity.getDestination();
    }

    public static final class Builder extends AbstractClientBuilder<SimpleDebeziumClient, SimpleCanalConnector> {

        @Override
        public SimpleDebeziumClient build(List<SimpleCanalConnector> connectors) {
            SimpleDebeziumClient debeziumClient = new SimpleDebeziumClient(connectors);
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
