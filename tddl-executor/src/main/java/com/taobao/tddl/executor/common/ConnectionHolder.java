package com.taobao.tddl.executor.common;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.DataSource;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

public class ConnectionHolder {

    private final static Logger      logger             = LoggerFactory.getLogger(ConnectionHolder.class);

    volatile private Set<Connection> connectionsToClose = new HashSet();
    volatile private Set<Connection> connections        = new HashSet();

    volatile private String          recentAccessGroup  = null;
    volatile private Connection      recentConnection   = null;

    private final ReentrantLock      lock               = new ReentrantLock();

    public ConnectionHolder(){

    }

    /**
     * @param groupName
     * @param ds
     * @param reuse 是否重用已有的
     * @return
     * @throws SQLException
     */
    public Connection getConnection(String groupName, DataSource ds, boolean reuse) throws SQLException {
        lock.lock();
        try {

            if (IDataNodeExecutor.USE_LAST_DATA_NODE.equals(groupName)) {
                groupName = this.recentAccessGroup;
                reuse = true;

                if (groupName == null) {
                    throw new TddlRuntimeException("recent access group is null, you cannot execute this sql");

                }
            }

            Connection conn = null;

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
                        throw new TddlRuntimeException("impossible!");
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

    public void tryClose(Connection conn) throws SQLException {

        // my_jdbchandler有时候会被重复调用
        if (conn.isClosed()) {
            return;
        }
        lock.lock();
        try {
            connectionsToClose.add(conn);

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
            return recentAccessGroup;
        } finally {
            lock.unlock();
        }
    }

    public Set<Connection> getAllConnection() {
        return this.connections;
    }

}
