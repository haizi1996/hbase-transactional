package com.hailin.transactional.cp;

import com.google.protobuf.Service;
import com.hailin.transactional.columns.ColumnMutation;
import com.hailin.transactional.columns.ColumnUtil;
import com.hailin.transactional.columns.TransactionTTL;
import com.hailin.transactional.master.ThemisMasterObserver;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

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
            String themisEnable = store.getColumnFamilyDescriptor().getValue(ThemisMasterObserver.THEMIS_ENABLE_KEY.getBytes());
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
        return false;
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
