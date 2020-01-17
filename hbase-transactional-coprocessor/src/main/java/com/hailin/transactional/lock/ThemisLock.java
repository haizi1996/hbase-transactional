package com.hailin.transactional.lock;

import java.io.ByteArrayInputStream;

import com.hailin.transactional.columns.ColumnCoordinate;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
public abstract class ThemisLock implements Writable {

    protected Type type = Type.Minimum;

    protected long timestamp;

    protected String clientAddress;

    protected long wallTime ;

    protected ColumnCoordinate columnCoordinate; // need not to be serialized


    public static ThemisLock parseFromByte(byte[] data)throws IOException{
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
        boolean isPrimary = in.readBoolean();
        ThemisLock lock = null;
        if (isPrimary) {
            lock = new PrimaryLock();
            lock.readFields(in);
        } else {
            lock = new SecondaryLock();
            lock.readFields(in);
        }
        return lock;
    }

    private void readFields(DataInputStream in) throws IOException {
        this.type = Type.codeToType(in.readByte());
        this.timestamp = in.readLong();
        this.clientAddress = Bytes.toString(Bytes.readByteArray(in));
        this.wallTime = in.readLong();
    }

    public abstract boolean isPrimary() ;

    public void write(DataOutput out) throws IOException {
        out.writeByte(type.getCode());
        out.writeLong(timestamp);
        Bytes.writeByteArray(out, Bytes.toBytes(clientAddress));
        out.writeLong(wallTime);
    }


    @Override
    public boolean equals(Object object) {
        if (!(object instanceof ThemisLock)) {
            return false;
        }
        ThemisLock lock = (ThemisLock)object;
        return this.type == lock.type && this.timestamp == lock.timestamp
                && this.wallTime == lock.wallTime && this.clientAddress.equals(lock.clientAddress);
    }

    @Override
    public String toString() {
        return "type=" + this.type + "/timestamp=" + this.timestamp + "/wallTime=" + this.wallTime
                + "/clientAddress=" + this.clientAddress + "/column=" + this.columnCoordinate;
    }
}
