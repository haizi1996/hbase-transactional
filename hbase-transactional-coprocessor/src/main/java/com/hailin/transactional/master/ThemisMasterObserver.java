package com.hailin.transactional.master;

import com.google.common.collect.Lists;
import com.hailin.transactional.columns.Column;
import com.hailin.transactional.columns.ColumnCoordinate;
import com.hailin.transactional.columns.ColumnUtil;
import com.hailin.transactional.columns.TransactionTTL;
import com.hailin.transactional.cp.ServerLockCleaner;
import com.hailin.transactional.lock.ThemisLock;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.mapreduce.Cluster;
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

    protected ServerLockCleaner lockCleaner;

    public void start(CoprocessorEnvironment env) throws IOException {
        ColumnUtil.init(env.getConfiguration());

        if(env.getConfiguration().getBoolean(THEMIS_EXPIRED_DATA_CLEAN_ENABLE_KEY , true)){
            TransactionTTL.init(env.getConfiguration());
//            ThemisCpStatistics.init(env.getConfiguration());
//            startExpiredTimestampCalculator((MasterCoprocessorHost.MasterEnvironmentForCoreCoprocessors) env);
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

    public static boolean isThemisEnableFamily(ColumnFamilyDescriptor columnFamilyDescriptor) {
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

    /**
     * 请求时间戳之前的锁
     * @param ts 指定时间戳
     * @throws IOException
     */
    public void cleanLockBeforeTimestamp(long ts) throws IOException{

        List<TableName> tableNames = getThemisTables(connection);
        for (TableName tableName : tableNames){
            LOGGER.error("start to clean expired lock for themis table : " + tableName);
            int cleanedLockCount = 0;
            Table table = connection.getTable(tableName);
            Scan scan = new Scan();
            scan.addFamily(ColumnUtil.LOCK_FAMILY_NAME);
            scan.setTimeRange(0, ts);
            ResultScanner scanner = table.getScanner(scan);
            Result result = null;
            while ((result = scanner.next()) != null) {
                for (Cell cell : result.listCells()) {
                    ThemisLock lock = ThemisLock.parseFromByte(cell.getValueArray());
                    Column dataColumn = ColumnUtil.getDataColumnFromConstructedQualifier(new Column(cell.getFamilyArray(),
                            cell.getQualifierArray()));
                    lock.setColumnCoordinate(new ColumnCoordinate(tableName.toBytes(), cell.getRowArray(),
                            dataColumn.getFamily(), dataColumn.getQualifier()));
                    lockCleaner.cleanLock(lock);
                    ++cleanedLockCount;
                    LOGGER.info("themis clean expired lock, lockTs=" + cell.getTimestamp() + ", expiredTs=" + ts
                            + ", lock=" + ThemisLock.parseFromByte(cell.getValueArray()));
                }
            }
            scanner.close();
            LOGGER.info("finish clean expired lock for themis table:" + tableName + ", cleanedLockCount="
                    + cleanedLockCount);
        }

    }

    public static List<TableName> getThemisTables(Connection connection) throws IOException{
        List<TableName> tableNames = Lists.newArrayList();
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();

            for (TableDescriptor tableDescriptor : tableDescriptors) {
                if(isThemisEnableTable(tableDescriptor)){
                    tableNames.add(tableDescriptor.getTableName());
                }
            }
        }catch (IOException e){
            LOGGER.error("get table names fail", e);
            throw e;
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
        return tableNames;
    }

    public static boolean isThemisEnableTable(TableDescriptor tableDescriptor) {
        for (ColumnFamilyDescriptor columnDesc : tableDescriptor.getColumnFamilies()) {
            if (isThemisEnableFamily(columnDesc)) {
                return true;
            }
        }
        return false;
    }
}
