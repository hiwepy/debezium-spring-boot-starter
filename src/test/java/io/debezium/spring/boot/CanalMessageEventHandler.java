package io.debezium.spring.boot;

import com.alibaba.fastjson.JSONObject;
import io.debezium.annotation.CanalEventHandler;
import io.debezium.annotation.event.*;
import io.debezium.model.DebeziumModel;
import io.debezium.protocol.DebeziumEntry;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@CanalEventHandler
@Slf4j
public class CanalMessageEventHandler {

    @OnCreateTableEvent(schema = "my_auth")
    public void onCreateTableEvent(DebeziumModel model, DebeziumEntry.RowChange rowChange) {
        log.info("onCreateTableEvent");
    }

    @OnCreateTableEvent(schema = "my_auth")
    public void onCreateTableEvent2(DebeziumModel model, DebeziumEntry.RowChange rowChange) {
        log.info("onCreateTableEvent2");
    }

    @OnCreateIndexEvent(schema = "my_auth", table = "user_info")
    public void onCreateIndexEvent(DebeziumModel model, DebeziumEntry.RowChange rowChange) {
        log.info("OnCreateIndexEvent");
    }

    @OnInsertEvent(schema = "my_auth", table = "user_info")
    public void onEventInsertData(DebeziumModel model, DebeziumEntry.RowChange rowChange) {

        // 1，获取当前事件的操作类型
        DebeziumEntry.EventType eventType = rowChange.getEventType();
        // 2,获取数据集
        List<DebeziumEntry.RowData> rowDatasList = rowChange.getRowDatasList();
        // 3,遍历RowDataList，并打印数据集
        for (DebeziumEntry.RowData rowData : rowDatasList) {
            JSONObject beforeData = new JSONObject();
            List<DebeziumEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
            for (DebeziumEntry.Column column : beforeColumnsList) {
                beforeData.put(column.getName(), column.getValue());
            }
            JSONObject affterData = new JSONObject();
            List<DebeziumEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            for (DebeziumEntry.Column column : afterColumnsList) {
                affterData.put(column.getName(), column.getValue());
            }

            System.out.println("Table:" + model.getTable() +
                    ",EventType:" + eventType +
                    ",Before:" + beforeData +
                    ",After:" + affterData);
        }

    }

    @OnUpdateEvent(schema = "my_auth", table = "user_info")
    public void onEventUpdateData(DebeziumModel model, DebeziumEntry.RowChange rowChange) {
        log.info("onEventUpdateData");
    }

    @OnDeleteEvent(schema = "my_auth", table = "user_info")
    public void onEventDeleteData(DebeziumEntry.RowChange rowChange, DebeziumModel model) {
        log.info("onEventDeleteData");
    }

}
