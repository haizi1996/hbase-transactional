package com.hailin.transactional.regionserver;


import com.hailin.transactional.columns.ColumnUtil;
import com.hailin.transactional.master.ThemisMasterObserver;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ThemisRegionObserver implements RegionObserver , Coprocessor {


    private  static final Logger LOGGER = LoggerFactory.getLogger(ThemisRegionObserver.class);

    public static final String THEMIS_DELETE_THEMIS_DELETED_DATA_WHEN_COMPACT = "themis.delete.themis.deleted.data.when.compact";
    // themis 单一预写列限定符
    public static final String SINGLE_ROW_PRIMARY_QUALIFIER = "_themisSingleRowPrewritePrimaryQualifier_";
    //是否允许清除过期数据
    private boolean expiredDataCleanEnable;

    //删除themis附带的数据 当删除的数据在合并的时候
    protected boolean deleteThemisDeletedDataWhenCompact;

    public void start(CoprocessorEnvironment env) throws IOException {
        ColumnUtil.init(env.getConfiguration());
        expiredDataCleanEnable = env.getConfiguration().getBoolean(
                ThemisMasterObserver.THEMIS_EXPIRED_DATA_CLEAN_ENABLE_KEY, true);
        deleteThemisDeletedDataWhenCompact =  env.getConfiguration().getBoolean(
                THEMIS_DELETE_THEMIS_DELETED_DATA_WHEN_COMPACT, false);
        if (expiredDataCleanEnable) {
            LOGGER.info("themis expired data clean enable, deleteThemisDeletedDataWhenCompact=" + deleteThemisDeletedDataWhenCompact);
        }
    }

    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {

        byte[] primaryQualifer = put.getAttribute(SINGLE_ROW_PRIMARY_QUALIFIER);

    }



























}
