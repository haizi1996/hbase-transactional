package com.hailin.transactional.columns;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

public class ColumnUtil {
    public static final char PRESERVED_COLUMN_CHARACTER = '#'; // must check column family don't contain this character
    public static final byte[] PRESERVED_COLUMN_CHARACTER_BYTES = Bytes.toBytes("" + PRESERVED_COLUMN_CHARACTER);
    public static final String PUT_QUALIFIER_SUFFIX = PRESERVED_COLUMN_CHARACTER + "p";
    public static final byte[] PUT_QUALIFIER_SUFFIX_BYTES = Bytes.toBytes(PUT_QUALIFIER_SUFFIX);
    public static final String DELETE_QUALIFIER_SUFFIX = PRESERVED_COLUMN_CHARACTER + "d";
    public static final byte[] DELETE_QUALIFIER_SUFFIX_BYTES = Bytes.toBytes(DELETE_QUALIFIER_SUFFIX);
    public static final String PRESERVED_QUALIFIER_SUFFIX = PUT_QUALIFIER_SUFFIX + " or " + DELETE_QUALIFIER_SUFFIX;
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


    public static boolean isPreservedColumn(Column column) {
        return containPreservedCharacter(column) || isLockColumn(column) || isPutColumn(column)
                || isDeleteColumn(column);
    }
    public static boolean isDeleteColumn(Column column) {
        return isDeleteColumn(column.getFamily(), column.getQualifier());
    }

    public static boolean isDeleteColumn(byte[] family, byte[] qualifier) {
        if (isCommitToSameFamily()) {
            return isQualifierWithSuffix(qualifier, DELETE_QUALIFIER_SUFFIX_BYTES);
        } else {
            return Bytes.equals(DELETE_FAMILY_NAME_BYTES, family);
        }
    }
    public static boolean containPreservedCharacter(Column column) {
        for (int i = 0; i < column.getFamily().length; ++i) {
            if (PRESERVED_COLUMN_CHARACTER_BYTES[0] == column.getFamily()[i]) {
                return true;
            }
        }
        if (isCommitToSameFamily()) {
            for (int i = 0; i < column.getQualifier().length; ++i) {
                if (PRESERVED_COLUMN_CHARACTER_BYTES[0] == column.getQualifier()[i]) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isPutColumn(Column column) {
        if (isCommitToSameFamily()) {
            return isQualifierWithSuffix(column.getQualifier(), PUT_QUALIFIER_SUFFIX_BYTES);
        } else {
            return Bytes.equals(PUT_FAMILY_NAME_BYTES, column.getFamily());
        }
    }

    protected static boolean isQualifierWithSuffix(byte[] qualifier, byte[] suffix) {
        for (int i = 1; i <= suffix.length; ++i) {
            if (i > qualifier.length) {
                return false;
            }
            if (qualifier[qualifier.length - i] != suffix[suffix.length - i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean isLockColumn(Column column) {
        if (column.getFamily() == null) {
            return false;
        }
        if (Bytes.equals(LOCK_FAMILY_NAME, column.getFamily())) {
            return true;
        }
        return false;
    }

    public static Column getDataColumnFromConstructedQualifier(Column lockColumn){
        byte[] constructedQualifier = lockColumn.getQualifier();
        if (constructedQualifier == null) {
            // TODO : throw exception or log an error
            return lockColumn;
        }
        int index = -1;
        for (int i = 0; i < constructedQualifier.length; ++i) {
            // the first PRESERVED_COLUMN_CHARACTER_BYTES exist in lockQualifier is the delimiter
            if (PRESERVED_COLUMN_CHARACTER_BYTES[0] == constructedQualifier[i]) {
                index = i;
                break;
            }
        }
        if (index <= 0) {
            return lockColumn;
        } else {
            byte[] family = new byte[index];
            byte[] qualifier = new byte[constructedQualifier.length - index - 1];
            System.arraycopy(constructedQualifier, 0, family, 0, index);
            System.arraycopy(constructedQualifier, index + 1, qualifier, 0, constructedQualifier.length
                    - index - 1);
            return new Column(family, qualifier);
        }
    }
}
