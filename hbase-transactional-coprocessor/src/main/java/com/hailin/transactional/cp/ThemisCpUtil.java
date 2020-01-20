package com.hailin.transactional.cp;

import com.hailin.transactional.columns.Column;
import com.hailin.transactional.columns.ColumnUtil;
import org.apache.hadoop.hbase.client.Get;

public class ThemisCpUtil {
    public static void addWriteColumnToGet(Column column, Get get) {
        Column putColumn = ColumnUtil.getPutColumn(column);
        if (ColumnUtil.isCommitToSameFamily() || !(get.getFamilyMap().containsKey(ColumnUtil.PUT_FAMILY_NAME_BYTES) && get.getFamilyMap().get(ColumnUtil.PUT_FAMILY_NAME_BYTES) == null)){
            get.addColumn(putColumn.getFamily() , putColumn.getQualifier());
        }
        Column deleteColumn = ColumnUtil.getDeleteColumn(column);
        if (ColumnUtil.isCommitToSameFamily()
                || !(get.getFamilyMap().containsKey(ColumnUtil.DELETE_FAMILY_NAME_BYTES) && get
                .getFamilyMap()
                .get(ColumnUtil.DELETE_FAMILY_NAME_BYTES) == null)) {
            get.addColumn(deleteColumn.getFamily(), deleteColumn.getQualifier());
        }

    }
}
