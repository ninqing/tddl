package com.taobao.tddl.executor.spi;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;

/**
 * 事务对象
 * 
 * @author mengshi.sunmengshi 2013-11-27 下午4:00:49
 * @since 5.0.0
 */
public interface ITransaction {

    public enum RW {
        READ, WRITE
    }

    public enum DistributedTransaction {
        /**
         * 禁止任何事务中的跨库操作
         */
        DEFAULT,
        /**
         * 允许跨库的读
         */
        ALLOW_READ_CROSS_DB
    }

    long getId();

    void commit() throws TddlException;

    void rollback() throws TddlException;

    boolean isAutoCommit();

    public ITHLog getHistoryLog();

    public void close() throws TddlException;

    public void setAutoCommit(boolean autoCommit);

    void setExecutionContext(ExecutionContext executionContext);

    void beginTransaction() throws TddlException;

}
