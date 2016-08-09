package com.taobao.tddl.repo.mysql.spi;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import javax.sql.DataSource;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.utils.ExceptionErrorCodeUtils;
import com.taobao.tddl.executor.common.AtomicNumberCreator;
import com.taobao.tddl.executor.common.ConnectionHolder;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.spi.ITHLog;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * @author mengshi.sunmengshi 2013-12-6 上午11:31:29
 * @since 5.0.0
 */
public class My_Transaction implements ITransaction {

    protected final static Logger     logger                = LoggerFactory.getLogger(My_Transaction.class);
    private final AtomicNumberCreator idGen                 = AtomicNumberCreator.getNewInstance();
    private final Integer             id                    = idGen.getIntegerNextNumber();

    /**
     * 当前进行事务的节点
     */
    protected String                  transactionalNodeName = null;
    protected boolean                 autoCommit            = true;
    private ExecutionContext          executionContext;

    public My_Transaction(ExecutionContext ec) throws TddlException{
        this.executionContext = ec;

        this.setAutoCommit(ec.isAutoCommit());
    }

    @Override
    public void beginTransaction() {
        ConnectionHolder ch = executionContext.getConnectionHolder();
        if (this.autoCommit) {
            for (Connection conn : ch.getAllConnection()) {
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                    throw new TddlRuntimeException(e);
                }
            }
        } else {
            transactionalNodeName = null;
            // 开启事务前，清理所有已有连接
            ch.closeAllConnections();
        }
    }

    /**
     * 策略两种：1. 强一致策略，事务中不允许跨机查询。2.弱一致策略，事务中允许跨机查询；
     * 
     * @param groupName
     * @param ds
     * @param strongConsistent 这个请求是否是强一致的，这个与ALLOW_READ一起作用。
     * 当ALLOW_READ的情况下，strongConsistent =
     * true时，会创建事务链接，而如果sConsistent=false则会创建非事务链接
     * @return
     */
    public Connection getConnection(String groupName, DataSource ds) throws SQLException {
        if (groupName == null) {
            throw new IllegalArgumentException("group name is null");
        }

        if (autoCommit) {// 自动提交，不建立事务链接
            return this.executionContext.getConnectionHolder().getConnection(groupName, ds, false);
        }
        Connection conn = null;
        if (transactionalNodeName != null) {// 已经有事务链接了
            if (transactionalNodeName.equalsIgnoreCase(groupName)
                || IDataNodeExecutor.USE_LAST_DATA_NODE.equals(groupName)) {
                conn = this.executionContext.getConnectionHolder().getConnection(transactionalNodeName, ds, true);
            } else {
                throw new RuntimeException("只支持单机事务，当前进行事务的是" + transactionalNodeName + " . 你现在希望进行操作的db是：" + groupName);
            }

            conn.setAutoCommit(false);
        } else {// 没有事务建立，新建事务
            transactionalNodeName = groupName;
            conn = getConnection(groupName, ds);

        }
        return conn;
    }

    @Override
    public void commit() throws TddlException {
        if (logger.isDebugEnabled()) {
            logger.debug("commit");
        }
        Set<Connection> conns = executionContext.getConnectionHolder().getAllConnection();

        for (Connection conn : conns) {
            try {
                conn.commit();
            } catch (SQLException e) {
                throw new TddlException(ExceptionErrorCodeUtils.UNKNOWN_EXCEPTION, e);
            }
        }

        beginTransaction();
    }

    @Override
    public void rollback() throws TddlException {

        if (logger.isDebugEnabled()) {
            logger.debug("rollback");
        }
        Set<Connection> conns = executionContext.getConnectionHolder().getAllConnection();

        for (Connection conn : conns) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                throw new TddlException(ExceptionErrorCodeUtils.UNKNOWN_EXCEPTION, e);
            }
        }
        beginTransaction();
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public ITHLog getHistoryLog() {
        return null;
    }

    @Override
    public void close() throws TddlException {
        return;
    }

    @Override
    public boolean isAutoCommit() {
        return autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        if (this.autoCommit == autoCommit) {
            return;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("setAutoCommit:" + autoCommit);
        }
        this.autoCommit = autoCommit;
        beginTransaction();
    }

    @Override
    public void setExecutionContext(ExecutionContext executionContext) {
        this.executionContext = executionContext;

    }

}
