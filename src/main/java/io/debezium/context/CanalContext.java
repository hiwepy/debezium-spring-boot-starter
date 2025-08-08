package io.debezium.context;

import io.debezium.model.DebeziumModel;
import com.alibaba.ttl.TransmittableThreadLocal;

/**
 * canal上下文
 */
public class CanalContext {

    private static TransmittableThreadLocal<DebeziumModel> threadLocal = new TransmittableThreadLocal<>();

    public static DebeziumModel getModel(){
        return threadLocal.get();
    }


    public static void setModel(DebeziumModel canalModel){
        threadLocal.set(canalModel);
    }


    public  static void removeModel(){
        threadLocal.remove();
    }
}
