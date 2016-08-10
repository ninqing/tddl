package com.taobao.tddl.executor.common;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import com.taobao.tddl.common.jdbc.IConnection;
import com.taobao.tddl.common.jdbc.IDataSource;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.exception.ExecutorException;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

public class ConnectionHolder {

    private final static Logger       logger             = LoggerFactory.getLogger(ConnectionHolder.class);

    volatile private Set<IConnection> connectionsToClose = new HashSet();
    volatile private Set<IConnection> connections        = new HashSet();

    volatile private String           recentAccessGroup  = null;
    volatile private IConnection      recentConnection   = null;

    private final ReentrantLock       lock               = new ReentrantLock();

    private boolean                   closed             = false;

    public ConnectionHolder(){

    }

    /**
     * @param groupName
     * @param ds
     * @param reuse 是否重用已有的
     * @return
     * @throws SQLException
     */
    public IConnection getConnection(String groupName, IDataSource ds, boolean reuse) throws SQLException {
        lock.lock();
        try {
            checkClosed();
            if (IDataNodeExecutor.USE_LAST_DATA_NODE.equals(groupName)) {
                groupName = this.recentAccessGroup;
                reuse = true;
                if (groupName == null) {
                    throw new ExecutorException("recent access group is null, you cannot execute this sql");
                }
            }

            IConnection conn = null;

            if (reuse) {
                // reuse只有两种情况
                // 1。事务，事务开启时会清空所有连接
                // 2.指定使用recent access group
                // 这两种情况的group name都会和recent access group相同
                if (recentAccessGroup != null) {
                    if (recentAccessGroup.equals(groupName)) {
                        conn = recentConnection;
                        connectionsToClose.remove(conn);
                    } else {
                        throw new ExecutorException("impossible!");
                    }
                }

            }

            if (conn == null) {
                conn = ds.getConnection();
                this.connections.add(conn);
            }

            recentAccessGroup = groupName;
            this.recentConnection = conn;
            return conn;
        } finally {
            lock.unlock();
        }

    }

    /**
     * 无条件关闭所有连接
     * 
     * @throws SQLException
     */
    public void closeAllConnections() {
        lock.lock();
        try {

            for (Connection conn : this.connections) {
                try {
                    conn.close();
                } catch (Exception e) {
                    logger.error("connection close failed, connection is " + conn, e);
                }
            }
            connections.clear();
            this.recentAccessGroup = null;
            this.recentConnection = null;
            this.connectionsToClose.clear();
        } finally {
            lock.unlock();
        }

    }

    public void tryClose(IConnection conn) throws SQLException {

        lock.lock();
        try {
            // my_jdbchandler有时候会被重复调用
            if (conn != null && conn.isClosed()) {
                return;
            }

            if (conn != null) {
                connectionsToClose.add(conn);
            }
            List<Connection> connsClosed = new ArrayList();
            for (Connection connToClose : this.connectionsToClose) {
                if (!connToClose.isClosed() && connToClose != recentConnection) {
                    try {
                        connToClose.close();
                    } catch (Exception e) {
                        logger.error("connection close failed, connection is " + connToClose, e);
                    } finally {
                        connsClosed.add(connToClose);

                    }
                }

            }

            connectionsToClose.removeAll(connsClosed);
            connections.removeAll(connsClosed);
        } finally {
            lock.unlock();
        }
    }

    public String getRecentAccessGroup() {

        lock.lock();

        try {
            checkClosed();
            return recentAccessGroup;
        } finally {
            lock.unlock();
        }
    }

    public Set<IConnection> getAllConnection() {

        return this.connections;
    }

    public void kill() throws SQLException {

        lock.lock();

        try {
            for (IConnection conn : this.connections) {
                try {
                    conn.kill();
                } catch (Exception e) {
                    logger.error("connection close failed, connection is " + conn, e);
                }
            }

            this.closeAllConnections();
            this.closed = true;
        } finally {
            lock.unlock();
        }
    }

    public void cancel() throws SQLException {

        lock.lock();

        try {
            for (IConnection conn : this.connections) {
                try {
                    conn.cancleQuery();
                } catch (Exception e) {
                    logger.error("connection close failed, connection is " + conn, e);
                }
            }

            this.tryClose(null);

            this.closed = true;
        } finally {
            lock.unlock();
        }
    }

    private void checkClosed() {
        if (!this.closed) {
            return;
        }

        throw new ExecutorException("connection holder is closed, cannot do any operations");
    }

    /**
     * 查询cancle掉之后，下次新的sql还能执行
     */
    public void restart() {
        this.closed = false;

    }

}
