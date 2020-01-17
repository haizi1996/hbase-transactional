package com.hailin.transactional.columns;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class Column implements Writable , Comparable<Column> {

    protected byte[] family;

    protected byte[] qualifier;

    public Column(Column column) {
        this.family = column.family;
        this.qualifier = column.qualifier;
    }

    @Override
    public int compareTo(Column other) {
        int ret = Bytes.compareTo(this.family, other.family);
        if (ret == 0) {
            return Bytes.compareTo(this.qualifier, other.qualifier);
        }
        return ret;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Bytes.writeByteArray(dataOutput , family);
        Bytes.writeByteArray(dataOutput , qualifier);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        family = Bytes.readByteArray(dataInput);
        qualifier = Bytes.readByteArray(dataInput);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Column)) {
            return false;
        }
        Column column = (Column) other;
        return Bytes.equals(this.family, column.getFamily())
                && Bytes.equals(this.qualifier, column.getQualifier());
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(family);
        result = 31 * result + Arrays.hashCode(qualifier);
        return result;
    }

    @Override
    public String toString() {
        return "family=" + Bytes.toString(family) + "/qualifier=" + Bytes.toStringBinary(qualifier);
    }

}
