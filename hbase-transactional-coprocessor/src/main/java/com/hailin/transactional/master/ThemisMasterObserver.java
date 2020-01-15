package com.hailin.transactional.master;

import com.hailin.transactional.columns.ColumnUtil;
import com.hailin.transactional.columns.TransactionTTL;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ThemisMasterObserver implements MasterObserver , Coprocessor {


    private static final Logger LOGGER = LoggerFactory.getLogger(ThemisMasterObserver.class);

    public static final String THEMIS_EXPIRED_DATA_CLEAN_ENABLE_KEY = "themis.expired.data.clean.enable";

    public static final String RETURNED_THEMIS_TABLE_DESC = "__themis.returned.table.desc__"; // support truncate table
    public static final String THEMIS_ENABLE_KEY = "THEMIS_ENABLE";


    protected Connection connection;

    protected ZKWatcher zkWatcher;

    protected HMaster hMaster;

    protected String themisExpiredTsZNodePath;

    public void start(CoprocessorEnvironment env) throws IOException {
        ColumnUtil.init(env.getConfiguration());

        if(env.getConfiguration().getBoolean(THEMIS_EXPIRED_DATA_CLEAN_ENABLE_KEY , true)){
            TransactionTTL.init(env.getConfiguration());
//            ThemisCpStatistics.init(env.getConfiguration());
            env.getInstance()
            startExpiredTimestampCalculator((MasterCoprocessorHost.MasterEnvironmentForCoreCoprocessors) env);
        }
    }

    public void stop(CoprocessorEnvironment env) throws IOException {

        if(Objects.nonNull(connection)){
            connection.close();
        }
    }

    @Override
    public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableDescriptor desc, RegionInfo[] regions) throws IOException {
        if (isReturnedThemisTableDesc(desc)){
            return;
        }
        boolean themisEnable = false;
        for (ColumnFamilyDescriptor columnFamilyDescriptor: desc.getColumnFamilies()) {
            if (isThemisEnableFamily(columnFamilyDescriptor)) {
                themisEnable = true;
                break;
            }
        }
        if (themisEnable) {
            for (ColumnFamilyDescriptor columnDesc : desc.getColumnFamilies()) {
                if (Bytes.equals(ColumnUtil.LOCK_FAMILY_NAME, columnDesc.getName())) {
                    throw new DoNotRetryIOException("family '" + ColumnUtil.LOCK_FAMILY_NAME_STRING
                            + "' is preserved by themis when " + THEMIS_ENABLE_KEY
                            + " is true, please change your family name");
                }
                // make sure TTL and MaxVersion is not set by user
                if (columnDesc.getTimeToLive() != HConstants.FOREVER) {
                    throw new DoNotRetryIOException("can not set TTL for family '"
                            + columnDesc.getNameAsString() + "' when " + THEMIS_ENABLE_KEY + " is true, TTL="
                            + columnDesc.getTimeToLive());
                }
                if (columnDesc.getMaxVersions() != HColumnDescriptor.DEFAULT_VERSIONS
                        && columnDesc.getMaxVersions() != Integer.MAX_VALUE) {
                    throw new DoNotRetryIOException("can not set MaxVersion for family '"
                            + columnDesc.getNameAsString() + "' when " + THEMIS_ENABLE_KEY
                            + " is true, MaxVersion=" + columnDesc.getMaxVersions());
                }
                ((ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor) columnDesc).setMaxVersions(Integer.MAX_VALUE);
            }
        }
        // 增加一列族 给表
        ((TableDescriptorBuilder.ModifyableTableDescriptor)desc).setColumnFamily(createLockFamily());
        LOGGER.info("add family '" + ColumnUtil.LOCK_FAMILY_NAME_STRING + "' for table:" + desc.getTableName().getNameAsString());
        if (!ColumnUtil.isCommitToSameFamily()) {
            addCommitFamilies(desc);
            LOGGER.info("add commit family '" + ColumnUtil.PUT_FAMILY_NAME + "' and '"
                    + ColumnUtil.DELETE_FAMILY_NAME + "' for table:" + desc.getTableName().getNameAsString());
        }

    }

    private void addCommitFamilies(TableDescriptor desc) {
        for (byte[] family : ColumnUtil.COMMIT_FAMILY_NAME_BYTES) {
            ((TableDescriptorBuilder.ModifyableTableDescriptor)desc).setColumnFamily(getCommitFamily(family));
        }
    }

    private ColumnFamilyDescriptor getCommitFamily(byte[] familyName) {
        ColumnFamilyDescriptorBuilder builder = ColumnFamilyDescriptorBuilder.newBuilder(familyName)
                .setTimeToLive(HConstants.FOREVER).setMaxVersions(Integer.MAX_VALUE);
        return builder.build();
    }

    private ColumnFamilyDescriptor createLockFamily() {
        ColumnFamilyDescriptorBuilder builder = ColumnFamilyDescriptorBuilder
                .newBuilder(ColumnUtil.LOCK_FAMILY_NAME)
                .setInMemory(true)
                .setMaxVersions(1)
                .setTimeToLive(HConstants.FOREVER);
        return builder.build();
    }

    private boolean isThemisEnableFamily(ColumnFamilyDescriptor columnFamilyDescriptor) {
        Bytes value = columnFamilyDescriptor.getValue(new Bytes(THEMIS_ENABLE_KEY.getBytes()));
        return  Boolean.parseBoolean(value.toString());
    }

    protected static boolean isReturnedThemisTableDesc(TableDescriptor descriptor){
        return Boolean.parseBoolean(descriptor.getValue(RETURNED_THEMIS_TABLE_DESC));
    }

    @Override
    public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx, List<TableName> tableNamesList, List<TableDescriptor> descriptors, String regex) throws IOException {
        for (TableDescriptor tableDescriptor : descriptors){
            setReturnedThemisTableDesc(tableDescriptor);
        }
    }

    private void setReturnedThemisTableDesc(TableDescriptor tableDescriptor) {
        ((TableDescriptorBuilder.ModifyableTableDescriptor)tableDescriptor).setValue(RETURNED_THEMIS_TABLE_DESC, "true");
    }
}
