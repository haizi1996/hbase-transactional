package com.hailin.transactional.cp;

import com.hailin.transactional.lock.PrimaryLock;
import com.hailin.transactional.lock.SecondaryLock;
import com.hailin.transactional.lock.ThemisLock;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.hbase.client.Connection;

/**
 * 服务端锁清除
 * @author zhanghailin
 */

@RequiredArgsConstructor
public class ServerLockCleaner {

    protected final Connection connection;

    /**
     * 清除锁，无论提交事务还是删除锁
     * @param lock
     */
    public void cleanLock(ThemisLock lock){
        long beginTs = System.nanoTime();
        try {
            PrimaryLock primaryLock = getPrimaryLockWithColumn(lock);
        }

    }

    private PrimaryLock getPrimaryLockWithColumn(ThemisLock lock) {
        if(lock.isPrimary()){
            return (PrimaryLock)lock;
        }else {
            PrimaryLock primaryLock = new PrimaryLock();
            primaryLock.setColumnCoordinate(((SecondaryLock)lock).getPrimaryColumn());
            ThemisLock.
            return primaryLock;
        }

    }
}
