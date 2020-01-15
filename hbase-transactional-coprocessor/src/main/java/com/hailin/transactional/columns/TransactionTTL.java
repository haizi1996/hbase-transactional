package com.hailin.transactional.columns;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class TransactionTTL {

    public static final String THEMIS_TRANSACTION_TTL_ENABLE_KEY = "themis.transaction.ttl.enable";
    public static final String THEMIS_READ_TRANSACTION_TTL_KEY = "themis.read.transaction.ttl";
    public static final int DEFAULT_THEMIS_READ_TRANSACTION_TTL = 86400; // in second
    public static final String THEMIS_WRITE_TRANSACTION_TTL_KEY = "themis.write.transaction.ttl";
    public static final int DEFAULT_THEMIS_WRITE_TRANSACTION_TTL = 60; // in second
    public static final String THEMIS_TRANSACTION_TTL_TIME_ERROR_KEY = "themis.transaction.ttl.time.error";
    public static final int DEFAULT_THEMIS_TRANSACTION_TTL_TIME_ERROR = 10; // in second

    public static int readTransactionTTL;
    public static int writeTransactionTTL;
    public static int transactionTTLTimeError;
    public static TimestampType timestampType = TimestampType.CHRONOS;
    public static boolean transactionTTLEnable;

    // 时间戳类型
    public static enum TimestampType {
        CHRONOS,
        // 毫秒
        MS
    }

    public static void init(Configuration conf) throws IOException {
        transactionTTLEnable = conf.getBoolean(THEMIS_TRANSACTION_TTL_ENABLE_KEY, true);
        readTransactionTTL = conf.getInt(THEMIS_READ_TRANSACTION_TTL_KEY,
                DEFAULT_THEMIS_READ_TRANSACTION_TTL) * 1000;
        writeTransactionTTL = conf.getInt(THEMIS_WRITE_TRANSACTION_TTL_KEY,
                DEFAULT_THEMIS_WRITE_TRANSACTION_TTL) * 1000;
        transactionTTLTimeError = conf.getInt(THEMIS_TRANSACTION_TTL_TIME_ERROR_KEY,
                DEFAULT_THEMIS_TRANSACTION_TTL_TIME_ERROR) * 1000;
        if (readTransactionTTL < writeTransactionTTL + transactionTTLTimeError) {
            throw new IOException(
                    "it is not reasonable to set readTransactionTTL just equal to writeTransactionTTL, readTransactionTTL="
                            + readTransactionTTL
                            + ", writeTransactionTTL="
                            + writeTransactionTTL
                            + ", transactionTTLTimeError=" + transactionTTLTimeError);
        }
    }
}
