package io.debezium.embedded.protocol;

import lombok.Getter;

import java.util.List;

public class DebeziumEntry {


    /**
     * Protobuf enum {@code io.debezium.protocol.EntryType}
     *
     * <pre>
     **打散后的事件类型，主要用于标识事务的开始，变更数据，结束*
     * </pre>
     */
    public enum EntryType  {
        /**
         * <code>TRANSACTIONBEGIN = 1;</code>
         */
        TRANSACTIONBEGIN(0, 1),
        /**
         * <code>ROWDATA = 2;</code>
         */
        ROWDATA(1, 2),
        /**
         * <code>TRANSACTIONEND = 3;</code>
         */
        TRANSACTIONEND(2, 3),
        /**
         * <code>HEARTBEAT = 4;</code>
         *
         * <pre>
         ** 心跳类型，内部使用，外部暂不可见，可忽略 *
         * </pre>
         */
        HEARTBEAT(3, 4),
        /**
         * <code>GTIDLOG = 5;</code>
         */
        GTIDLOG(4, 5),
        ;

        /**
         * <code>TRANSACTIONBEGIN = 1;</code>
         */
        public static final int TRANSACTIONBEGIN_VALUE = 1;
        /**
         * <code>ROWDATA = 2;</code>
         */
        public static final int ROWDATA_VALUE = 2;
        /**
         * <code>TRANSACTIONEND = 3;</code>
         */
        public static final int TRANSACTIONEND_VALUE = 3;
        /**
         * <code>HEARTBEAT = 4;</code>
         *
         * <pre>
         ** 心跳类型，内部使用，外部暂不可见，可忽略 *
         * </pre>
         */
        public static final int HEARTBEAT_VALUE = 4;
        /**
         * <code>GTIDLOG = 5;</code>
         */
        public static final int GTIDLOG_VALUE = 5;


        public final int getNumber() { return value; }

        public static EntryType valueOf(int value) {
            return switch (value) {
                case 1 -> TRANSACTIONBEGIN;
                case 2 -> ROWDATA;
                case 3 -> TRANSACTIONEND;
                case 4 -> HEARTBEAT;
                case 5 -> GTIDLOG;
                default -> null;
            };
        }


        private static final EntryType[] VALUES = values();

        @Getter
        private final int index;
        @Getter
        private final int value;

        EntryType(int index, int value) {
            this.index = index;
            this.value = value;
        }

    }
    /**
     * <pre>
     ** 事件类型 *
     * </pre>
     */
    public enum EventType{
        /**
         * <code>INSERT = 1;</code>
         */
        INSERT(0, 1),
        /**
         * <code>UPDATE = 2;</code>
         */
        UPDATE(1, 2),
        /**
         * <code>DELETE = 3;</code>
         */
        DELETE(2, 3),
        /**
         * <code>CREATE = 4;</code>
         */
        CREATE(3, 4),
        /**
         * <code>ALTER = 5;</code>
         */
        ALTER(4, 5),
        /**
         * <code>ERASE = 6;</code>
         */
        ERASE(5, 6),
        /**
         * <code>QUERY = 7;</code>
         */
        QUERY(6, 7),
        /**
         * <code>TRUNCATE = 8;</code>
         */
        TRUNCATE(7, 8),
        /**
         * <code>RENAME = 9;</code>
         */
        RENAME(8, 9),
        /**
         * <code>CINDEX = 10;</code>
         *
         * <pre>
         **CREATE INDEX*
         * </pre>
         */
        CINDEX(9, 10),
        /**
         * <code>DINDEX = 11;</code>
         */
        DINDEX(10, 11),
        /**
         * <code>GTID = 12;</code>
         */
        GTID(11, 12),
        /**
         * <code>XACOMMIT = 13;</code>
         *
         * <pre>
         ** XA *
         * </pre>
         */
        XACOMMIT(12, 13),
        /**
         * <code>XAROLLBACK = 14;</code>
         */
        XAROLLBACK(13, 14),
        /**
         * <code>MHEARTBEAT = 15;</code>
         *
         * <pre>
         ** MASTER HEARTBEAT *
         * </pre>
         */
        MHEARTBEAT(14, 15),
        ;

        public static EventType valueOf(int value) {
            return switch (value) {
                case 1 -> INSERT;
                case 2 -> UPDATE;
                case 3 -> DELETE;
                case 4 -> CREATE;
                case 5 -> ALTER;
                case 6 -> ERASE;
                case 7 -> QUERY;
                case 8 -> TRUNCATE;
                case 9 -> RENAME;
                case 10 -> CINDEX;
                case 11 -> DINDEX;
                case 12 -> GTID;
                case 13 -> XACOMMIT;
                case 14 -> XAROLLBACK;
                case 15 -> MHEARTBEAT;
                default -> null;
            };
        }

        private static final EventType[] VALUES = values();

        @Getter
        private final int index;
        @Getter
        private final int value;

        EventType(int index, int value) {
            this.index = index;
            this.value = value;
        }

    }

}
