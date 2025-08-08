package io.debezium.annotation.event;

import io.debezium.annotation.OnCanalEvent;
import io.debezium.protocol.DebeziumEntry;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

/**
 * 表结构发生变化，新增时，先判断数据库实例是否存在，不存在则创建
 *
 * @author lujun
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@OnCanalEvent(eventType = DebeziumEntry.EventType.CREATE)
public @interface OnCreateTableEvent {

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
}
