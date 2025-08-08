package io.debezium.util;

import io.debezium.client.impl.ClusterCanalConnector;
import io.debezium.client.impl.ClusterNodeAccessStrategy;
import io.debezium.client.impl.SimpleCanalConnector;
import io.debezium.client.impl.SimpleNodeAccessStrategy;
import io.debezium.client.kafka.KafkaCanalConnector;
import io.debezium.client.kafka.KafkaOffsetCanalConnector;
import io.debezium.client.pulsarmq.PulsarMQCanalConnector;
import io.debezium.client.rabbitmq.RabbitMQCanalConnector;
import io.debezium.client.rocketmq.RocketMQCanalConnector;
import io.debezium.common.zookeeper.ZkClientx;
import io.debezium.spring.boot.*;
import org.springframework.util.StringUtils;

import java.net.InetSocketAddress;

public class ConnectorUtil {

    /**
     * 创建集群模式的 Canal 连接器
     * @param instance 实例配置
     * @return Canal 连接器
     */
    public static ClusterCanalConnector createClusterCanalConnector(DebeziumClusterProperties.Instance instance) {
        if (StringUtils.hasText(instance.getZkServers())) {
            ClusterCanalConnector canalConnector = new ClusterCanalConnector(instance.getUsername(),
                    instance.getPassword(),
                    instance.getDestination(),
                    new ClusterNodeAccessStrategy(instance.getDestination(), ZkClientx.getZkClient(instance.getZkServers())));
            canalConnector.setSoTimeout(instance.getSoTimeout());
            canalConnector.setIdleTimeout(instance.getIdleTimeout());
            canalConnector.setRetryTimes(instance.getRetryTimes());
            canalConnector.setRetryInterval(instance.getRetryInterval());
            return canalConnector;
        }
        ClusterCanalConnector canalConnector = new ClusterCanalConnector(
                instance.getUsername(),
                instance.getPassword(),
                instance.getDestination(),
                new SimpleNodeAccessStrategy(AddressUtils.parseAddresses(instance.getAddresses())));
        canalConnector.setSoTimeout(instance.getSoTimeout());
        canalConnector.setIdleTimeout(instance.getIdleTimeout());
        canalConnector.setRetryTimes(instance.getRetryTimes());
        canalConnector.setRetryInterval(instance.getRetryInterval());
        return canalConnector;
    }

    /**
     * 创建 Kafka 连接器
     * @param instance 实例配置
     * @return Canal 连接器
     */
    public static KafkaCanalConnector createKafkaCanalConnector(DebeziumKafkaClientProperties.Instance instance) {
        KafkaCanalConnector connector = instance.isEarliest() ? new KafkaOffsetCanalConnector(instance.getServers(),
                instance.getTopic(),  instance.getPartition(), instance.getGroupId(),
                Boolean.TRUE) : new KafkaCanalConnector(instance.getServers(),
                instance.getTopic(),  instance.getPartition(), instance.getGroupId(),
                instance.getBatchSize(), Boolean.TRUE);
        return connector;
    }

    /**
     * 创建 PulsarMQ 连接器
     * @param instance 实例配置
     * @return Canal 连接器
     */
    public static PulsarMQCanalConnector createPulsarMQCanalConnector(DebeziumPulsarClientProperties.Instance instance) {
        PulsarMQCanalConnector connector = new PulsarMQCanalConnector(Boolean.TRUE,
                instance.getServiceUrl(), instance.getRoleToken(), instance.getTopic(),
                instance.getSubscriptName(), instance.getBatchSize(), instance.getBatchTimeoutSeconds(),
                instance.getBatchProcessTimeoutSeconds(), instance.getRedeliveryDelaySeconds(),
                instance.getAckTimeoutSeconds(),
                instance.isRetry(), instance.isRetryDLQUpperCase(), instance.getMaxRedeliveryCount());
        return connector;
    }

    /**
     * 创建 RabbitMQ 连接器
     * @param instance 实例配置
     * @return Canal 连接器
     */
    public static RabbitMQCanalConnector createRabbitMQCanalConnector(DebeziumRabbitmqClientProperties.Instance instance) {
        RabbitMQCanalConnector connector = new RabbitMQCanalConnector(instance.getAddresses(), instance.getVhost(),
                instance.getQueueName(), instance.getAccessKey(), instance.getSecretKey(),
                instance.getUsername(), instance.getPassword(), instance.getResourceOwnerId(),
                Boolean.TRUE);
        return connector;
    }

    /**
     * 创建 RocketMQ 连接器
     * @param instance 实例配置
     * @return Canal 连接器
     */
    public static RocketMQCanalConnector createRocketMQCanalConnector(DebeziumRocketmqClientProperties.Instance instance) {
        // 1、创建连接实例
        RocketMQCanalConnector connector;
        if (StringUtils.hasText(instance.getAccessKey()) && StringUtils.hasText(instance.getSecretKey())) {
            if (StringUtils.hasText(instance.getNamespace())) {
                connector = new RocketMQCanalConnector(instance.getNameServer(), instance.getTopic(),
                        instance.getGroupName(), instance.getAccessKey(), instance.getSecretKey(),
                        instance.getBatchSize(), Boolean.TRUE, instance.isEnableMessageTrace(), null,
                        instance.getAccessChannel(), instance.getNamespace());
            } else if (StringUtils.hasText(instance.getCustomizedTraceTopic())) {
                connector = new RocketMQCanalConnector(instance.getNameServer(), instance.getTopic(),
                        instance.getGroupName(), instance.getAccessKey(), instance.getSecretKey(),
                        instance.getBatchSize(), Boolean.TRUE, instance.isEnableMessageTrace(),
                        instance.getCustomizedTraceTopic(), instance.getAccessChannel());
            } else {
                connector = new RocketMQCanalConnector(instance.getNameServer(), instance.getTopic(),
                        instance.getGroupName(), instance.getAccessKey(), instance.getSecretKey(),
                        instance.getBatchSize(), Boolean.TRUE);
            }
        } else {
            connector = new RocketMQCanalConnector(instance.getNameServer(), instance.getTopic(),
                    instance.getGroupName(), instance.getBatchSize(), Boolean.TRUE);
        }
        return connector;
    }

    /**
     * 创建单机模式的 Canal 连接器
     * @param instance 实例配置
     * @return Canal 连接器
     */
    public static SimpleCanalConnector createSimpleCanalConnector(DebeziumSimpleProperties.Instance instance) {
        InetSocketAddress address = new InetSocketAddress(instance.getHost(), instance.getPort());
        SimpleCanalConnector canalConnector = new SimpleCanalConnector(address,
                instance.getUsername(),
                instance.getPassword(),
                instance.getDestination());
        canalConnector.setSoTimeout(instance.getSoTimeout());
        canalConnector.setIdleTimeout(instance.getIdleTimeout());
        return canalConnector;
    }

}
