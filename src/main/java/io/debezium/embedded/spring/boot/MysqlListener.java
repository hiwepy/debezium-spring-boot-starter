package io.debezium.embedded.spring.boot;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Executors;

@Slf4j
@Component
public class MysqlListener {

    private final List<DebeziumEngine<ChangeEvent<String, String>>> engineList = new ArrayList<>();

    private MysqlListener(@Qualifier("mysqlProperties") List<Properties> list) {
        for (Properties props : list) {
            this.engineList.add(DebeziumEngine.create(Json.class)
                    .using(props)
                    .notifying(record -> {
                        receiveChangeEvent(record.value(), props);
                    }).build());
        }
    }

    private void receiveChangeEvent(String value, Properties props) {
        if (Objects.nonNull(value)) {
            try {
                // 解析JSON字符串
                JSONObject jsonValue = JSON.parseObject(value);
                JSONObject payload = jsonValue.getJSONObject("payload");
                if (payload != null) {
                    ChangeDataMessage message = new ChangeDataMessage();
                    // 设置操作类型
                    String handleType = JSON.parseObject(JSON.toJSONString(payload.get("op")), String.class);
                    message.setDataType(handleType);
                    // 设置变更前后的数据
                    JSONObject beforeData = payload.getJSONObject("before");
                    if (beforeData != null) {
                        message.setBeforeData(beforeData.toJSONString());
                    }
                    JSONObject afterData = payload.getJSONObject("after");
                    if (afterData != null) {
                        message.setAfterData(afterData.toJSONString());
                    }
                    // 设置数据库名称和表名称
                    JSONObject source = payload.getJSONObject("source");
                    if (source != null) {
                        message.setDatabaseName(source.getString("db"));
                        message.setTableName(source.getString("table"));
                        // 设置数据库类型为MySQL
                        message.setDbType(props.getProperty("database.dbType"));
                        // 设置偏移量
                        Long offset = source.getLong("pos");
                        if (offset != null) {
                            message.setOffset(offset);
                        }
                    }
                    // 这里可以添加对message的后续处理，例如发送到消息队列等
                    log.info("解析变更事件成功: {}", message);
                    SyncDataStrategy strategy = syncDataStrategyRouter.switchStrategy(message.getTableName());
                    if(Objects.isNull(strategy)){
                        log.error("未找到当前数据表的处理器");
                    }else{
                        strategy.syncTableData(message);
                    }
                }
            } catch (Exception e) {
                log.error("解析变更事件失败: {}", e.getMessage());
            }
        }
    }

    @PostConstruct
    private void start() {
        for (DebeziumEngine<ChangeEvent<String, String>> engine : engineList) {
            Executors.newSingleThreadExecutor().execute(engine);
        }
    }

    @PreDestroy
    private void stop() {
        for (DebeziumEngine<ChangeEvent<String, String>> engine : engineList) {
            if (engine != null) {
                try {
                    engine.close();
                } catch (IOException e) {
                }
            }
        }
    }
}
