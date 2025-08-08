package io.debezium.annotation.event;

import io.debezium.annotation.OnCanalEvent;
import io.debezium.protocol.DebeziumEntry;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

/**
 * 刪除索引操作
 *
 * @author lujun
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@OnCanalEvent(eventType = DebeziumEntry.EventType.DINDEX)
public @interface OnDropIndexEvent {
    /**
     * canal 指令
     * default for all
     * @return canal destination
     */
    @AliasFor(annotation = OnCanalEvent.class)
    String destination() default "";

    /**
     * 数据库实例
     * @return 数据库实例
     */
    @AliasFor(annotation = OnCanalEvent.class)
    String schema();

    /**
     * 监听的表
     * default for all
     * @return 监听的表
     */
    @AliasFor(annotation = OnCanalEvent.class)
    String table();
}
