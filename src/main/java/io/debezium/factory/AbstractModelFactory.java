package io.debezium.factory;


import io.debezium.enums.TableNameEnum;
import io.debezium.handler.EntryHandler;
import io.debezium.util.GenericUtil;
import io.debezium.util.HandlerUtil;

public abstract class AbstractModelFactory<T> implements IModelFactory<T> {

    @Override
    public <R> R newInstance(EntryHandler entryHandler, T t) throws Exception {
        String canalTableName = HandlerUtil.getCanalTableNameCombination(entryHandler);
        if (TableNameEnum.ALL.name().toLowerCase().equals(canalTableName)) {
            return (R) t;
        }
        Class<R> tableClass = GenericUtil.getTableClass(entryHandler);
        if (tableClass != null) {
            return newInstance(tableClass, t);
        }
        return null;
    }

    abstract <R> R newInstance(Class<R> tableClass, T t) throws Exception;
}
