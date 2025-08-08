package io.debezium.annotation.event;

import io.debezium.annotation.OnCanalEvent;
import io.debezium.protocol.DebeziumEntry;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

/**
 * 重命名表
 *
 * @author lujun
 */

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@OnCanalEvent(eventType = DebeziumEntry.EventType.RENAME)
public @interface OnRenameTableEvent {

    /**
     * canal 指令
     * default for all
     * @return canal 指令
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
