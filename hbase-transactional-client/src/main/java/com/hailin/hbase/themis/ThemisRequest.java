package com.hailin.hbase.themis;

import com.hailin.transactional.columns.Column;
import com.hailin.transactional.columns.ColumnUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class ThemisRequest {

    protected static void checkContainingPreservedColumn(byte[] family, byte[] qualifier) throws IOException {

        Column column = new Column(family, qualifier == null ? HConstants.EMPTY_BYTE_ARRAY : qualifier);
        if (ColumnUtil.isPreservedColumn(column)) {
            throw new IOException("can not query preserved column : " + column);
        }
    }

    // check the request must contain at least one column
    public static void checkContainColumn(ThemisRequest request) throws IOException {
        if (!request.hasColumn()) {
            throw new IOException("must set at least one column for themis request class : "
                    + request.getClass());
        }
    }

    protected abstract boolean hasColumn();
}

    abstract class ThemisMutation extends ThemisRequest{

        public abstract Map<byte [], List<Cell>> getFamilyMap();

        protected boolean hasColumn() {
            return getFamilyMap() != null && getFamilyMap().size() != 0;
        }

        public static void checkContainingPreservedColumns(Map<byte[], List<Cell>> mutations)
                throws IOException {
            for (Map.Entry<byte[], List<Cell>> entry : mutations.entrySet()) {
                for (Cell ce : entry.getValue()) {
                    checkContainingPreservedColumn(ce.getFamilyArray(), ce.getQualifierArray());
                }
            }
        }
    }
