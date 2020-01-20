package com.hailin.transactional.cp;

import com.google.common.collect.Lists;
import com.google.protobuf.Service;
import com.hailin.transactional.columns.Column;
import com.hailin.transactional.columns.ColumnMutation;
import com.hailin.transactional.columns.ColumnUtil;
import com.hailin.transactional.columns.TransactionTTL;
import com.hailin.transactional.lock.ThemisLock;
import com.hailin.transactional.master.ThemisMasterObserver;
import com.hailin.transactional.regionserver.ThemisRegionObserver;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Getter
@Setter
public class ThemisProtocolImpl implements ThemisProtocol {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThemisProtocolImpl.class);

    private static final byte[] EMPTY_BYTES = new byte[0];

    private CoprocessorEnvironment env;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        try {

            ColumnUtil.init(env.getConfiguration());
            TransactionTTL.init(env.getConfiguration());
            ThemisCpStatistics.init(env.getConfiguration());
        }catch (Exception e){
            throw new RuntimeException(e);
        }


    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {

    }

    @Override
    public Iterable<Service> getServices() {
        return null;
    }

    @Override
    public Result themisGet(Get get, long startTs, boolean ignoreLock) throws IOException {
        return null;
    }

    @Override
    public byte[][] prewriteRow(byte[] row, List<ColumnMutation> mutations, long prewriteTs, byte[] secondaryLock, byte[] primaryLock, int primaryIndex) throws IOException {
        return prewriteRow(row, mutations, prewriteTs, secondaryLock, primaryLock, primaryIndex, false);
    }

    public byte[][] prewriteRow(final byte[] row, final List<ColumnMutation> mutations,
                                final long prewriteTs, final byte[] secondaryLock, final byte[] primaryLock,
                                final int primaryIndex, final boolean singleRow) throws IOException {
        checkFamily(mutations);

        final long beginTs = System.nanoTime();
        checkWriterTTL(System.currentTimeMillis() , prewriteTs , row);
            checkPrimaryLockAndIndex(primaryLock, primaryIndex);
            return new MutationCallable<byte[][]>(row){
                @Override
                public byte[][] doMutation(Region region, Region.RowLock rowLock) throws IOException {
                    // 检查锁冲突
                    for (ColumnMutation mutation :  mutations) {
                        byte[][] conflict = checkPrewriterConflict(region , row , rowLock , mutation , prewriteTs);
                        if (Objects.nonNull(conflict)){
                            return conflict;
                        }
                    }
                    Put prewritePut = new Put(row);
                    byte[] primaryQualifier = null;

                    for (int i = 0; i < mutations.size() ; i++) {
                        boolean isPrimary = false;
                        ColumnMutation mutation = mutations.get(i);
                        byte[] lockBytes = secondaryLock;
                        if(Objects.nonNull(primaryLock) && i == primaryIndex){
                            lockBytes = primaryLock;
                            isPrimary = true;
                        }
                        ThemisLock lock = ThemisLock.parseFromByte(lockBytes);
                        lock.setType(mutation.getType());
                        if (!singleRow && Objects.equals(lock.getType() , KeyValue.Type.Put)){
                            prewritePut.addColumn(mutation.getFamily() , mutation.getQualifier() , prewriteTs , mutation.getValue());
                        }
                        Column lockColumn = ColumnUtil.getLockColumn(mutation);
                        prewritePut.addColumn(lockColumn.getFamily() , lockColumn.getQualifier() , prewriteTs , ThemisLock.toByte(lock));
                        if (isPrimary) {
                            prewritePut.setAttribute( ThemisRegionObserver.SINGLE_ROW_PRIMARY_QUALIFIER , primaryQualifier);
                        }
                        mutateToRegion(region, row, Lists.<Mutation> newArrayList(prewritePut));
                        return null;
                    }



                    return new byte[0][];
                }
            }.run();


    }

    private void mutateToRegion(Region region, byte[] row, List<Mutation> mutations) throws IOException {
        region.mutateRow(RowMutations.of(mutations));
    }

    private byte[][] checkPrewriterConflict(Region region, byte[] row, Region.RowLock rowLock, Column column, long prewriteTs) throws IOException {
        Column lockColumn = ColumnUtil.getLockColumn(column);
        // 检查锁存在
        Get get = new Get(row).addColumn(lockColumn.getFamily() ,lockColumn.getQualifier());
        Result result = getFromRegion(region, get, rowLock);
        byte[] existLockBytes = result.isEmpty() ? null : result.listCells().get(0).getValueArray();
        boolean lockExpired = Objects.isNull(existLockBytes) ? false : isLockExpired(result.listCells().get(0).getTimestamp());

        //检查是不是有新的写入
        get = new Get(row);
        ThemisCpUtil.addWriteColumnToGet(column, get);
        get.setTimeRange(prewriteTs, Long.MAX_VALUE);

    }

    private Result getFromRegion(Region region, Get get, Region.RowLock rowLock) throws IOException {
        return region.get(get);
    }

    /**
     * 当lockBytes 为空是， primaryIndex必须是-1
     * @param lockBytes
     * @param primaryIndex
     * @throws IOException
     */
    private void checkPrimaryLockAndIndex(byte[] lockBytes, int primaryIndex) throws IOException {
        if ((lockBytes == null && primaryIndex != -1) || (lockBytes != null && primaryIndex == -1))
            throw new IOException("primaryLock is inconsistent with primaryIndex, primaryLock="
                    + ThemisLock.parseFromByte(lockBytes) + ", primaryIndex=" + primaryIndex);
    }

    /**
     * 检查写入的ttl
     * @param currentTimeMillis
     * @param startTs
     * @param row
     */
    private void checkWriterTTL(long currentTimeMillis, long startTs, byte[] row) {
        if (!TransactionTTL.transactionTTLEnable){
            return;
        }
        long expiredTimestamp = TransactionTTL.getExpiredTimestampForWrite(currentTimeMillis);
        if (startTs < expiredTimestamp) {
            throw new RuntimeException(
                    "Expired Write Transaction, write transaction start Ts:" + startTs + ", expired Ts:"
                            + expiredTimestamp + ", currentMs=" + currentTimeMillis + ", row=" + Bytes.toString(row));
        }

    }

    protected void checkFamily(List<ColumnMutation> mutations) throws IOException {
        byte[][] families = new byte[mutations.size()][];
        for (int i = 0; i < mutations.size(); i++) {
            families[i] = mutations.get(i).getFamily();
        }
        checkFamily(families);
    }
    protected void checkFamily(final byte[][] families) throws IOException {
        checkFamily(((RegionCoprocessorEnvironment) getEnv()).getRegion(), families);
    }

    protected static void checkFamily(Region region, byte[][] families) throws IOException {
        for (byte[] family : families) {
            Store store = region.getStore(family);
            if (store == null) {
                throw new DoNotRetryIOException("family : '" + Bytes.toString(family) + "' not found in table : "
                        + region.getTableDescriptor().getTableName().getNameAsString());
            }
            String themisEnable = store.getColumnFamilyDescriptor().getValue(ThemisMasterObserver.THEMIS_ENABLE_KEY.getBytes()).toString();
            if (themisEnable == null || !Boolean.parseBoolean(themisEnable)) {
                throw new DoNotRetryIOException("can not access family : '" + Bytes.toString(family)
                        + "' because " + ThemisMasterObserver.THEMIS_ENABLE_KEY + " is not set");
            }
        }
    }

    @Override
    public byte[][] prewriteSingleRow(byte[] row, List<ColumnMutation> mutations, long prewriteTs, byte[] secondaryLock, byte[] primaryLock, int primaryIndex) throws IOException {
        return new byte[0][];
    }

    @Override
    public boolean commitRow(byte[] row, List<ColumnMutation> mutations, long prewriteTs, long commitTs, int primaryIndex) throws IOException {
        return false;
    }

    @Override
    public boolean commitSingleRow(byte[] row, List<ColumnMutation> mutations, long prewriteTs, long commitTs, int primaryIndex) throws IOException {
        return false;
    }

    @Override
    public boolean isLockExpired(long lockTimestamp) throws IOException {
        if (!TransactionTTL.transactionTTLEnable) {
            return false;
        }
        long currentMs = System.currentTimeMillis();
        return lockTimestamp < TransactionTTL.getExpiredTimestampForWrite(currentMs);
    }

    @Override
    public byte[] getLockAndErase(byte[] row, byte[] family, byte[] column, long prewriteTs) throws IOException {
        return new byte[0];
    }

    abstract class MutationCallable<R> {
        private final byte[] row;
        public MutationCallable(byte[] row) {
            this.row = row;
        }

        public abstract R doMutation(Region region, Region.RowLock rowLock) throws IOException;

        public R run() throws IOException {
            Region region = ((RegionCoprocessorEnvironment) getEnv()).getRegion();
            Region.RowLock rowLock = region.getRowLock( row, false);
            // wait for all previous transactions to complete (with lock held)
            // region.getMVCC().completeMemstoreInsert(region.getMVCC().beginMemstoreInsert());
            try {
                return doMutation(region, rowLock);
            } finally {
                rowLock.release();
            }
        }
    }
}
