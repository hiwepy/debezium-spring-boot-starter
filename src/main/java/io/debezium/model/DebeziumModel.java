package io.debezium.model;


import io.debezium.protocol.DebeziumEntry;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * canal 消息模型
 */
@Setter
@Getter
@Builder
public class DebeziumModel {

    import lombok.Data;

    public class ChangeListenerModel {
        /**
         * 当前DB
         */
        private String db;
        /**
         * 当前表
         */
        private String table;
        /**
         * 操作类型 1 add 2 update 3 delete
         */
        private Integer eventType;
        /**
         * 操作时间
         */
        private Long changeTime;
        /**
         * 现数据
         */
        private String data;
        /**
         * 之前数据
         */
        private String beforeData;
    }

    /**
     * 消息id
     */
    private long id;

    /**
     * 库名
     */
    private String destination;

    /**
     * 库名
     */
    private String schema;
    /**
     * 表名
     */
    private String table;
    /**
     * 事件类型
     */
    private DebeziumEntry.EventType eventType;
    /**
     * binlog executeTime
     */
    private Long executeTime;

    /**
     * dml build timeStamp
     */
    private Long createTime;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CanalModel{");
        sb.append("id=").append(id);
        sb.append(", schema='").append(schema).append('\'');
        sb.append(", table='").append(table).append('\'');
        sb.append(", eventType='").append(eventType).append('\'');
        sb.append(", executeTime=").append(executeTime);
        sb.append(", createTime=").append(createTime);
        sb.append('}');
        return sb.toString();
    }

}
