package io.debezium.embedded.handler;

/**
 * 处理 Entry
 * @param <R> Entry
 */
public interface EntryHandler<R> {



    default void insert(R t) {

    }


    default void update(R before, R after) {

    }


    default void delete(R t) {

    }
}
