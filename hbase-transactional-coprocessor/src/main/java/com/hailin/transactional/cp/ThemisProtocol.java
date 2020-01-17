package com.hailin.transactional.cp;

import com.hailin.transactional.columns.ColumnMutation;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.List;

public interface ThemisProtocol extends Coprocessor  {

    /**
     * 在没有锁冲突和忽略锁的请求下，返回数据结果
     * @param get
     * @param startTs
     * @param ignoreLock
     * @return
     * @throws IOException
     */
    Result themisGet(final Get get, final long startTs, boolean ignoreLock) throws IOException;

    /**
     * 预写一行， 这一行包括主列
     * @param row
     * @param mutations
     * @param prewriteTs
     * @param secondaryLock
     * @param primaryLock
     * @param primaryIndex
     * @return
     * @throws IOException
     */
    byte[][] prewriteRow(final byte[] row, final List<ColumnMutation> mutations,
                        final long prewriteTs, final byte[] secondaryLock, final byte[] primaryLock,
                         final int primaryIndex) throws IOException;

    public byte[][] prewriteSingleRow(final byte[] row, final List<ColumnMutation> mutations,
                                      final long prewriteTs, final byte[] secondaryLock, final byte[] primaryLock,
                                      final int primaryIndex) throws IOException;

    boolean commitRow(final byte[] row, final List<ColumnMutation> mutations,
                      final long prewriteTs, final long commitTs, final int primaryIndex) throws IOException;

    public boolean commitSingleRow(final byte[] row, final List<ColumnMutation> mutations,
                                   final long prewriteTs, final long commitTs, final int primaryIndex) throws IOException;

    public boolean isLockExpired(final long lockTimestamp) throws IOException;

    // return null if lock not exist; otherwise, return lock and erase the lock
    public byte[] getLockAndErase(final byte[] row, final byte[] family, final byte[] column,
                                  final long prewriteTs) throws IOException;
}
