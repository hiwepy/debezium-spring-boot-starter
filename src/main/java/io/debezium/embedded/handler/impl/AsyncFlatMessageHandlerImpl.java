package io.debezium.embedded.handler.impl;


import io.debezium.embedded.handler.AbstractFlatMessageHandler;
import io.debezium.embedded.handler.EntryHandler;
import io.debezium.embedded.handler.RowDataHandler;
import io.debezium.embedded.protocol.DebeziumEntry;
import io.debezium.protocol.FlatMessage;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.List;
import java.util.Map;

public class AsyncFlatMessageHandlerImpl extends AbstractFlatMessageHandler {

    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    public AsyncFlatMessageHandlerImpl(List<? extends EntryHandler> entryHandlers,
                                       RowDataHandler<List<Map<String, String>>> rowDataHandler,
                                       ThreadPoolTaskExecutor threadPoolTaskExecutor) {
        super(null, entryHandlers, rowDataHandler);
        this.threadPoolTaskExecutor = threadPoolTaskExecutor;
    }

    public AsyncFlatMessageHandlerImpl(List<DebeziumEntry.EntryType> subscribeTypes,
                                       List<? extends EntryHandler> entryHandlers,
                                       RowDataHandler<List<Map<String, String>>> rowDataHandler,
                                       ThreadPoolTaskExecutor threadPoolTaskExecutor) {
        super(subscribeTypes, entryHandlers, rowDataHandler);
        this.threadPoolTaskExecutor = threadPoolTaskExecutor;
    }

    @Override
    public void handleMessage(String destination, FlatMessage flatMessage) {
        threadPoolTaskExecutor.execute(() -> super.handleMessage(destination, flatMessage));
    }


}
