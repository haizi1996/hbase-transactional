package com.hailin.transactional.columns;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

public class ColumnUtil {


    public static final String PUT_FAMILY_NAME = "#p";
    public static final byte[] PUT_FAMILY_NAME_BYTES = Bytes.toBytes(PUT_FAMILY_NAME);
    public static final String DELETE_FAMILY_NAME = "#d";
    public static final byte[] DELETE_FAMILY_NAME_BYTES = Bytes.toBytes(DELETE_FAMILY_NAME);
    public static final byte[][] COMMIT_FAMILY_NAME_BYTES = new byte[][] { PUT_FAMILY_NAME_BYTES,
            DELETE_FAMILY_NAME_BYTES };
    
    public static final String THEMIS_COMMIT_FAMILY_TYPE = "themis.commit.family.type";
    protected static CommitFamily commitFamily;

    //锁字段 列族
    public static final byte[] LOCK_FAMILY_NAME = Bytes.toBytes("L");
    public static final String LOCK_FAMILY_NAME_STRING = Bytes.toString(LOCK_FAMILY_NAME);



    /**
     * 提交的列租
     */
    public static enum CommitFamily{
        //包含数据列族
        SAME_WITH_DATA_FAMILY,

        DIFFERNT_FAMILY;
    }
    public static void init(Configuration configuration){
        commitFamily = CommitFamily.valueOf(configuration.get(THEMIS_COMMIT_FAMILY_TYPE , CommitFamily.DIFFERNT_FAMILY.toString()));
    }

    public static boolean isCommitToSameFamily() {
        return commitFamily == CommitFamily.SAME_WITH_DATA_FAMILY;
    }

    public static boolean isCommitToDifferentFamily() {
        return commitFamily == CommitFamily.DIFFERNT_FAMILY;
    }
}
