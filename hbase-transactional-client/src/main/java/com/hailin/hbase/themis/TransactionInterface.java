package com.hailin.hbase.themis;

import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;

/**
 * 事务的api接口
 * @author zhanghailin
 */
public interface TransactionInterface {

    Result get(byte[] tableName, ThemisGet get) throws IOException;
    void put(byte[] tableName, ThemisPut put) throws IOException;
    void delete(byte[] tableName, ThemisDelete delete) throws IOException;
    ThemisScanner getScanner(byte[] tableName, ThemisScan scan) throws IOException;
    void commit() throws IOException;
}
