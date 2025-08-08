package io.debezium.embedded.spring.boot;

import io.debezium.client.SimpleDebeziumClient;
import io.debezium.client.impl.SimpleCanalConnector;
import io.debezium.factory.EntryColumnModelFactory;
import io.debezium.handler.EntryHandler;
import io.debezium.handler.MessageHandler;
import io.debezium.handler.RowDataHandler;
import io.debezium.handler.impl.AsyncMessageHandlerImpl;
import io.debezium.handler.impl.RowDataHandlerImpl;
import io.debezium.handler.impl.SyncMessageHandlerImpl;
import io.debezium.protocol.DebeziumEntry;
import io.debezium.util.ConnectorUtil;
import lombok.extern.slf4j.Slf4j;
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
import java.util.stream.Collectors;

@Configuration
@ConditionalOnClass({ SimpleCanalConnector.class })
@ConditionalOnProperty(value = DebeziumProperties.CANAL_MODE, havingValue = "simple", matchIfMissing = true)
@EnableConfigurationProperties({DebeziumProperties.class, DebeziumSimpleProperties.class})
@Import(DebeziumThreadPoolAutoConfiguration.class)
@Slf4j
public class DebeziumSimpleClientAutoConfiguration {

    @Bean
    public RowDataHandler<DebeziumEntry.RowData> rowDataHandler() {
        return new RowDataHandlerImpl(new EntryColumnModelFactory());
    }

    @Bean
    @ConditionalOnProperty(value = DebeziumProperties.CANAL_ASYNC, havingValue = "true", matchIfMissing = true)
    public MessageHandler asyncMessageHandler(DebeziumProperties properties,
                                              RowDataHandler<DebeziumEntry.RowData> rowDataHandler,
                                              ObjectProvider<EntryHandler> entryHandlerProvider,
                                              @Qualifier("canalTaskExecutor") ThreadPoolTaskExecutor threadPoolTaskExecutor) {
        return new AsyncMessageHandlerImpl(properties.getSubscribeTypes(), entryHandlerProvider.stream().collect(Collectors.toList()), rowDataHandler, threadPoolTaskExecutor);
    }

    @Bean
    @ConditionalOnProperty(value = DebeziumProperties.CANAL_ASYNC, havingValue = "false")
    public MessageHandler syncMessageHandler(DebeziumProperties properties,
                                             RowDataHandler<DebeziumEntry.RowData> rowDataHandler,
                                             ObjectProvider<EntryHandler> entryHandlerProvider) {
        return new SyncMessageHandlerImpl(properties.getSubscribeTypes(), entryHandlerProvider.stream().collect(Collectors.toList()), rowDataHandler);
    }

    @Bean(initMethod = "start", destroyMethod = "stop")
    public SimpleDebeziumClient simpleCanalClient(ObjectProvider<SimpleCanalConnector> connectorProvider,
                                                  MessageHandler messageHandler,
                                                  DebeziumProperties debeziumProperties,
                                                  DebeziumSimpleProperties connectorProperties){
        // 1. 获取Spring 上下文中所有的 SimpleCanalConnector
        List<SimpleCanalConnector> simpleCanalConnectors = connectorProvider.stream().collect(Collectors.toList());
        // 2. 初始化配置文件中配置的 SimpleCanalConnector
        if(!CollectionUtils.isEmpty(connectorProperties.getInstances())){
            simpleCanalConnectors.addAll(connectorProperties.getInstances().stream()
                    .map(instance -> ConnectorUtil.createSimpleCanalConnector(instance))
                    .collect(Collectors.toList()));
        }
        // 3. 返回 SimpleCanalClient
        return (SimpleDebeziumClient) new SimpleDebeziumClient.Builder()
                .batchSize(debeziumProperties.getBatchSize())
                .filter(debeziumProperties.getFilter())
                .timeout(debeziumProperties.getTimeout())
                .unit(debeziumProperties.getUnit())
                .messageHandler(messageHandler)
                .setSubscribeTypes(debeziumProperties.getSubscribeTypes())
                .build(simpleCanalConnectors);
    }

}
