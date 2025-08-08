package io.debezium.spring.boot;

import io.debezium.handler.EntryHandler;
import org.springframework.stereotype.Component;

@Component
public class CanalMessageEntryHandler implements EntryHandler<UserInfo> {

    @Override
    public void insert(UserInfo t) {
    }

    @Override
    public void update(UserInfo before, UserInfo after) {
    }

    @Override
    public void delete(UserInfo t) {
    }

}
