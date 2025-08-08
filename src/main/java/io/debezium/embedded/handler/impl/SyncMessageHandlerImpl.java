package io.debezium.embedded.handler.impl;


import io.debezium.embedded.handler.AbstractMessageHandler;
import io.debezium.embedded.handler.EntryHandler;
import io.debezium.embedded.handler.RowDataHandler;
import io.debezium.embedded.protocol.DebeziumEntry;
import io.debezium.protocol.Message;

import java.util.List;

/**
 * 同步处理 Message
 */
public class SyncMessageHandlerImpl extends AbstractMessageHandler {


    public SyncMessageHandlerImpl(List<? extends EntryHandler> entryHandlers,
                                  RowDataHandler<DebeziumEntry.RowData> rowDataHandler) {
        super(null, entryHandlers, rowDataHandler);
    }

    public SyncMessageHandlerImpl(List<DebeziumEntry.EntryType> subscribeTypes,
                                  List<? extends EntryHandler> entryHandlers,
                                  RowDataHandler<DebeziumEntry.RowData> rowDataHandler) {
        super(subscribeTypes, entryHandlers, rowDataHandler);
    }

    @Override
    public void handleMessage(String destination, Message message) {
        super.handleMessage(destination, message);
    }


}
