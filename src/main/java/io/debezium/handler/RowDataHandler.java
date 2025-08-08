package io.debezium.handler;

import io.debezium.protocol.DebeziumEntry;

/**
 * 处理行数据
 * @param <T> 行数据
 */
public interface RowDataHandler<T> {

    <R> void handlerRowData(T t, EntryHandler<R> entryHandler, DebeziumEntry.EventType eventType) throws Exception;

}
