package io.debezium.embedded.spring.boot;

import io.debezium.client.RocketMQDebeziumClient;
import io.debezium.client.rocketmq.RocketMQCanalConnector;
import io.debezium.factory.MapColumnModelFactory;
import io.debezium.handler.EntryHandler;
import io.debezium.handler.MessageHandler;
import io.debezium.handler.RowDataHandler;
import io.debezium.handler.impl.AsyncFlatMessageHandlerImpl;
import io.debezium.handler.impl.MapRowDataHandlerImpl;
import io.debezium.handler.impl.SyncFlatMessageHandlerImpl;
import io.debezium.util.ConnectorUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Configuration
@ConditionalOnClass({ RocketMQCanalConnector.class, DefaultMQPushConsumer.class })
@ConditionalOnProperty(value = DebeziumProperties.CANAL_MODE, havingValue = "rocketmq")
@EnableConfigurationProperties({DebeziumProperties.class, DebeziumRocketmqClientProperties.class})
@Import(DebeziumThreadPoolAutoConfiguration.class)
@Slf4j
public class DebeziumRocketmqClientAutoConfiguration {

	@Bean
	public RowDataHandler<List<Map<String, String>>> rowDataHandler() {
		return new MapRowDataHandlerImpl(new MapColumnModelFactory());
	}

	@Bean
	@ConditionalOnProperty(value = DebeziumProperties.CANAL_ASYNC, havingValue = "true", matchIfMissing = true)
	public MessageHandler asyncMessageHandler(DebeziumProperties properties,
                                              RowDataHandler<List<Map<String, String>>> rowDataHandler,
                                              ObjectProvider<EntryHandler> entryHandlerProvider,
                                              @Qualifier("canalTaskExecutor") ThreadPoolTaskExecutor canalTaskExecutor) {
		return new AsyncFlatMessageHandlerImpl(properties.getSubscribeTypes(), entryHandlerProvider.stream().collect(Collectors.toList()), rowDataHandler, canalTaskExecutor);
	}

	@Bean
	@ConditionalOnProperty(value = DebeziumProperties.CANAL_ASYNC, havingValue = "false")
	public MessageHandler syncMessageHandler(DebeziumProperties properties,
                                             RowDataHandler<List<Map<String, String>>> rowDataHandler,
                                             ObjectProvider<EntryHandler> entryHandlerProvider) {
		return new SyncFlatMessageHandlerImpl(properties.getSubscribeTypes(), entryHandlerProvider.stream().collect(Collectors.toList()), rowDataHandler);
	}

	@Bean(initMethod = "start", destroyMethod = "stop")
	public RocketMQDebeziumClient rocketMQCanalClient(ObjectProvider<RocketMQCanalConnector> connectorProvider,
                                                      ObjectProvider<MessageHandler> messageHandlerProvider,
                                                      DebeziumProperties debeziumProperties,
                                                      DebeziumRocketmqClientProperties connectorProperties){
		// 1. 获取Spring 上下文中所有的 RocketMQCanalConnector
		List<RocketMQCanalConnector> rocketMQCanalConnectors = connectorProvider.stream().collect(Collectors.toList());
		// 2. 初始化配置文件中配置的 SimpleCanalConnector
		if(!CollectionUtils.isEmpty(connectorProperties.getInstances())){
			rocketMQCanalConnectors.addAll(connectorProperties.getInstances().stream()
					.map(instance -> ConnectorUtil.createRocketMQCanalConnector(instance))
					.collect(Collectors.toList()));
		}
		// 3. 返回 RocketMQCanalClient
		return (RocketMQDebeziumClient) new RocketMQDebeziumClient.Builder()
				.batchSize(debeziumProperties.getBatchSize())
				.filter(debeziumProperties.getFilter())
				.timeout(debeziumProperties.getTimeout())
				.unit(debeziumProperties.getUnit())
				.messageHandler(messageHandlerProvider.getIfAvailable())
				.build(rocketMQCanalConnectors);
	}

}
