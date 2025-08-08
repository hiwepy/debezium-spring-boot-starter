package io.debezium.embedded.client;

import io.debezium.handler.MessageHandler;
import io.debezium.protocol.FlatMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.util.List;

@Slf4j
public abstract class AbstractMQDebeziumClient<C extends CanalMQConnector> extends AbstractDebeziumClient<C> {

    public AbstractMQDebeziumClient(List<C> connectors) {
        super(connectors);
    }

    @Override
    public void process(C connector) {
        String destination = this.getDestination(connector);
        MessageHandler messageHandler = super.getMessageHandler();
        while (running) {
            try {
                connector.connect();
                connector.subscribe();
                while (running) {
                    try {
                        List<FlatMessage> messages = connector.getFlatListWithoutAck(timeout, unit);
                        if (CollectionUtils.isEmpty(messages)) {
                            continue;
                        }
                        for (FlatMessage flatMessage : messages) {
                            messageHandler.handleMessage(destination, flatMessage);
                        }
                        connector.ack();
                    } catch (Exception e) {
                        log.error("canal 消费异常", e);
                    }
                }
            } catch (Exception e) {
                log.error("canal 连接异常", e);
            }
        }
        connector.unsubscribe();
        connector.disconnect();
    }

}
