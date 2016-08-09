package com.taobao.tddl.repo.demo.spi;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.spi.ITHLog;
import com.taobao.tddl.executor.spi.ITransaction;

/**
 * @author danchen
 */
public class DemoTransaction implements ITransaction {

    private ExecutionContext executionContext;

    @Override
    public long getId() {
        return 0;
    }

    @Override
    public void commit() throws TddlException {
    }

    @Override
    public void rollback() throws TddlException {
    }

    @Override
    public boolean isAutoCommit() {

        return false;
    }

    @Override
    public ITHLog getHistoryLog() {
        return null;
    }

    @Override
    public void close() throws TddlException {
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
    }

    @Override
    public void beginTransaction() throws TddlException {
    }

    @Override
    public void setExecutionContext(ExecutionContext executionContext) {
        this.executionContext = executionContext;

    }

    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

}
