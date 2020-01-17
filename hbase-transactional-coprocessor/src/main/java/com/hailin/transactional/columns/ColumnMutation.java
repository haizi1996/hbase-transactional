package com.hailin.transactional.columns;


import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
public class ColumnMutation extends Column {


    protected KeyValue.Type type;
    protected byte[] value;

    public ColumnMutation() {}

    public ColumnMutation(Column column, KeyValue.Type type, byte[] value) {
        super(column);
        this.type = type;
        this.value = value;
    }

    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeByte(type.getCode());
        Bytes.writeByteArray(out, value);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        this.type = KeyValue.Type.codeToType(in.readByte());
        this.value = Bytes.readByteArray(in);
    }

    @Override
    public String toString() {
        return "column=" + super.toString() + ",\type=" + type;
    }

    public Cell toKeyValue(byte[] row, long timestamp) {
        return new KeyValue(row, family, qualifier, timestamp, type, value);
    }
}
