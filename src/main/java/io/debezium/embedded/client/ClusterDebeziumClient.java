package io.debezium.embedded.client;

import io.debezium.client.impl.ClusterCanalConnector;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.List;

/**
 * 集群模式 Canal 客户端
 */
public class ClusterDebeziumClient extends AbstractDebeziumClient<ClusterCanalConnector> {

    private ClusterDebeziumClient(List<ClusterCanalConnector> connectors) {
        super(connectors);
    }

    @Override
    protected String getDestination(ClusterCanalConnector connector) {
        Field destinationField =  ReflectionUtils.findField(ClusterCanalConnector.class, "destination");
        ReflectionUtils.makeAccessible(destinationField);
        return (String) ReflectionUtils.getField(destinationField, connector);
    }

    public static final class Builder extends AbstractClientBuilder<ClusterDebeziumClient, ClusterCanalConnector> {

        @Override
        public ClusterDebeziumClient build(List<ClusterCanalConnector> connectors) {
            ClusterDebeziumClient debeziumClient = new ClusterDebeziumClient(connectors);
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
