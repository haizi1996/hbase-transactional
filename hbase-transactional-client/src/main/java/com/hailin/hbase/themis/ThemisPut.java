package com.hailin.hbase.themis;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Setter
@Getter
public class ThemisPut extends ThemisMutation {

    private Put put;

    public ThemisPut(byte[] row){
        put = new Put(row);
    }

    public ThemisPut(Put put) throws IOException {
        checkContainingPreservedColumns(put.getFamilyCellMap());
        setHBasePut(put);
    }

    private void setHBasePut(Put put) {
        setPut(put);
    }

    @Override
    protected boolean hasColumn() {
        return MapUtils.isNotEmpty(put.getFamilyCellMap());
    }

    public Map<byte[], List<Cell>> getFamilyMap() {
        return put.getFamilyCellMap();
    }
}
