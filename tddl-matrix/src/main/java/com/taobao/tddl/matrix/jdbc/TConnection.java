package com.taobao.tddl.matrix.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.GroupDataSourceRouteHelper;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.exception.TddlNestableRuntimeException;
import com.taobao.tddl.common.jdbc.IConnection;
import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.common.plugin.PreSqlPlugin;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.MatrixExecutor;
import com.taobao.tddl.executor.common.ConnectionHolder;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.IResultSetCursor;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.spi.IGroupExecutor;
import com.taobao.tddl.group.utils.GroupHintParser;
import com.taobao.tddl.matrix.jdbc.utils.ExceptionUtils;
import com.taobao.tddl.optimizer.OptimizerContext;

/**
 * @author mengshi.sunmengshi 2013-11-22 下午3:26:06
 * @since 5.0.0
 */
public class TConnection implements IConnection {

    private final static Logger    logger               = LoggerFactory.getLogger(TConnection.class);

    private MatrixExecutor         executor             = null;
    private final TDataSource      ds;
    private ExecutionContext       executionContext     = new ExecutionContext();                                    // 记录上一次的执行上下文
    private final List<TStatement> openedStatements     = Collections.synchronizedList(new ArrayList<TStatement>(2));
    private boolean                isAutoCommit         = true;                                                      // jdbc规范，新连接为true
    private boolean                closed;
    private int                    transactionIsolation = -1;
    private final ExecutorService  executorService;
    /**
     * 管理这个连接下用到的所有物理连接
     */
    private ConnectionHolder       connectionHolder     = new ConnectionHolder();
    private long                   lastInsertId;

    public TConnection(TDataSource ds){
        this.ds = ds;
        this.executor = ds.getExecutor();
        this.executorService = ds.borrowExecutorService();
    }

    /**
     * 执行sql语句的逻辑
     */
    public ResultSet executeSQL(String sql, Parameters params, TStatement stmt, Map<String, Object> extraCmd,
                                ExecutionContext executionContext) throws SQLException {

        if (this.getConnectionHolder().getAllConnection().size() > 1) {
            throw new IllegalAccessError("连接泄露了也许");
        }

        this.connectionHolder.restart();
        List<PreSqlPlugin> plugins = this.ds.getPreSqlPluginList();

        if (!GeneralUtil.isEmpty(plugins)) {
            for (PreSqlPlugin plugin : plugins) {
                sql = plugin.handle(sql);
            }
        }

        ExecutorContext.setContext(this.ds.getConfigHolder().getExecutorContext());
        OptimizerContext.setContext(this.ds.getConfigHolder().getOptimizerContext());
        ResultCursor resultCursor;
        ResultSet rs = null;
        extraCmd.putAll(buildExtraCommand(sql));
        // 处理下group hint
        String groupHint = GroupHintParser.extractTDDLGroupHint(sql);
        if (!StringUtils.isEmpty(groupHint)) {
            sql = GroupHintParser.removeTddlGroupHint(sql);
            executionContext.setGroupHint(GroupHintParser.buildTddlGroupHint(groupHint));
        }
        executionContext.setExecutorService(executorService);
        executionContext.setParams(params);
        executionContext.setExtraCmds(extraCmd);
        try {
            resultCursor = executor.execute(sql, executionContext);
        } catch (TddlException e) {
            logger.error("error when executeSQL, sql is: " + sql, e);

            throw new TddlNestableRuntimeException(e);
        }

        if (resultCursor instanceof IResultSetCursor) {
            rs = ((IResultSetCursor) resultCursor).getResultSet();
        } else {
            rs = new TResultSet(resultCursor);
        }

        return rs;
    }

    @Override
    public TPreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        ExecutionContext context = prepareExecutionContext();
        TPreparedStatement stmt = new TPreparedStatement(ds, this, sql, context);
        openedStatements.add(stmt);
        return stmt;
    }

    @Override
    public TStatement createStatement() throws SQLException {
        checkClosed();
        ExecutionContext context = prepareExecutionContext();
        TStatement stmt = new TStatement(ds, this, context);
        openedStatements.add(stmt);
        return stmt;
    }

    private ExecutionContext prepareExecutionContext() throws SQLException {
        if (isAutoCommit) {
            if (this.executionContext != null) {
                this.executionContext.cleanTempTables();
            }

            // 即使为autoCommit也需要记录
            // 因为在JDBC规范中，只要在statement.execute执行之前,设置autoCommit=false都是有效的
            this.executionContext = new ExecutionContext();

        } else {
            if (this.executionContext == null) {
                this.executionContext = new ExecutionContext();
            }

            if (this.executionContext.isAutoCommit()) {
                this.executionContext.setAutoCommit(false);
            }
        }

        this.executionContext.setTxIsolation(transactionIsolation);
        this.executionContext.setConnectionHolder(this.connectionHolder);
        this.executionContext.setConnection(this);
        return this.executionContext;
    }

    /*
     * ========================================================================
     * JDBC事务相关的autoCommit设置、commit/rollback、TransactionIsolation等
     * ======================================================================
     */

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        if (this.isAutoCommit == autoCommit) {
            // 先排除两种最常见的状态,true==true 和false == false: 什么也不做
            return;
        }
        this.isAutoCommit = autoCommit;
        if (this.executionContext != null) {
            this.executionContext.setAutoCommit(autoCommit);
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return isAutoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        if (isAutoCommit) {
            return;
        }

        if (this.executionContext != null) {
            try {
                // 事务结束,清理事务内容
                this.executor.commit(this.executionContext);

                this.executionContext = null;
            } catch (TddlException e) {
                throw new SQLException(e);
            }
        }
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        if (isAutoCommit) {
            return;
        }

        if (this.executionContext != null) {
            try {
                this.executor.rollback(executionContext);

                this.executionContext = null;
            } catch (TddlException e) {
                throw new SQLException(e);
            }
        }
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new TDatabaseMetaData(ds);
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("No operations allowed after connection closed.");
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }

        List<SQLException> exceptions = new LinkedList<SQLException>();
        try {
            // 关闭statement
            for (TStatement stmt : openedStatements) {
                try {
                    stmt.close(false);
                } catch (SQLException e) {
                    exceptions.add(e);
                }
            }
        } finally {
            openedStatements.clear();
        }

        if (executorService != null) {
            this.ds.releaseExecutorService(executorService);
        }
        if (this.executionContext != null) {
            this.executionContext.cleanTempTables();
        }
        if (this.executionContext != null && this.executionContext.getTransaction() != null) {
            try {
                this.executionContext.getTransaction().close();
            } catch (TddlException e) {
                exceptions.add(new SQLException(e));
            }
        }

        this.connectionHolder.closeAllConnections();

        closed = true;
        ExceptionUtils.throwSQLException(exceptions, "close tconnection", Collections.EMPTY_LIST);
    }

    private Map<String, Object> buildExtraCommand(String sql) {
        Map<String, Object> extraCmd = new HashMap();
        String andorExtra = "/* ANDOR ";
        String tddlExtra = "/* TDDL ";
        if (sql != null) {
            String commet = TStringUtil.substringAfter(sql, tddlExtra);
            // 去掉注释
            if (TStringUtil.isNotEmpty(commet)) {
                commet = TStringUtil.substringBefore(commet, "*/");
            }

            if (TStringUtil.isEmpty(commet) && sql.startsWith(andorExtra)) {
                commet = TStringUtil.substringAfter(sql, andorExtra);
                commet = TStringUtil.substringBefore(commet, "*/");
            }

            if (TStringUtil.isNotEmpty(commet)) {
                String[] params = commet.split(",");
                for (String param : params) {
                    String[] keyAndVal = param.split("=");
                    if (keyAndVal.length != 2) {
                        throw new IllegalArgumentException(param + " is wrong , only key = val supported");
                    }
                    String key = keyAndVal[0];
                    String val = keyAndVal[1];
                    extraCmd.put(key, val);
                }
            }
        }
        extraCmd.putAll(this.ds.getConnectionProperties());
        return extraCmd;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        TStatement stmt = createStatement();
        stmt.setResultSetType(resultSetType);
        stmt.setResultSetConcurrency(resultSetConcurrency);
        return stmt;
    }

    @Override
    public TStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
                                                                                                            throws SQLException {
        TStatement stmt = (TStatement) createStatement(resultSetType, resultSetConcurrency);
        stmt.setResultSetHoldability(resultSetHoldability);
        return stmt;
    }

    @Override
    public TPreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        TPreparedStatement stmt = prepareStatement(sql);
        stmt.setAutoGeneratedKeys(autoGeneratedKeys);
        return stmt;
    }

    @Override
    public TPreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
                                               int resultSetHoldability) throws SQLException {
        TPreparedStatement stmt = prepareStatement(sql, resultSetType, resultSetConcurrency);
        stmt.setResultSetHoldability(resultSetHoldability);
        return stmt;
    }

    @Override
    public TPreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        TPreparedStatement stmt = prepareStatement(sql);
        stmt.setColumnIndexes(columnIndexes);
        return stmt;
    }

    @Override
    public TPreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        TPreparedStatement stmt = prepareStatement(sql);
        stmt.setColumnNames(columnNames);
        return stmt;
    }

    @Override
    public TPreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
                                                                                                       throws SQLException {
        TPreparedStatement stmt = prepareStatement(sql);
        stmt.setResultSetType(resultSetType);
        stmt.setResultSetConcurrency(resultSetConcurrency);
        return stmt;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return prepareCall(sql, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareCall(sql, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        checkClosed();
        // 针对存储过程，直接下推到default库上执行
        String defaultDbIndex = this.ds.getConfigHolder().getOptimizerContext().getRule().getDefaultDbIndex(null);
        IGroupExecutor groupExecutor = this.ds.getConfigHolder()
            .getExecutorContext()
            .getTopologyHandler()
            .get(defaultDbIndex);

        Object groupDataSource = groupExecutor.getRemotingExecutableObject();
        if (groupDataSource instanceof DataSource) {
            GroupDataSourceRouteHelper.executeByGroupDataSourceIndex(0);
            Connection conn = ((DataSource) groupDataSource).getConnection();
            CallableStatement target = null;
            if (resultSetType != Integer.MIN_VALUE && resultSetHoldability != Integer.MIN_VALUE) {
                target = conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
            } else if (resultSetType != Integer.MIN_VALUE) {
                target = conn.prepareCall(sql, resultSetType, resultSetConcurrency);
            } else {
                target = conn.prepareCall(sql);
            }

            TCallableStatement stmt = new TCallableStatement(ds, this, sql, null, target);
            openedStatements.add(stmt);
            return stmt;
        } else {
            throw new UnsupportedOperationException();
        }

    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (this.transactionIsolation == level) {
            return;
        }

        this.transactionIsolation = level;
        if (executionContext != null) {
            executionContext.setTxIsolation(level);
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return transactionIsolation;
    }

    /**
     * 暂时实现为isClosed
     */
    @Override
    public boolean isValid(int timeout) throws SQLException {
        return this.isClosed();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        /*
         * 如果你看到这里，那么恭喜，哈哈 mysql默认在5.x的jdbc driver里面也没有实现holdability 。
         * 所以默认都是.CLOSE_CURSORS_AT_COMMIT 为了简化起见，我们也就只实现close这种
         */
        throw new UnsupportedOperationException("setHoldability");
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.getClass().isAssignableFrom(iface);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return (T) this;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public boolean removeStatement(Object arg0) {
        return openedStatements.remove(arg0);
    }

    public ExecutionContext getExecutionContext() {
        return this.executionContext;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // do nothing
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        // do nothing
    }

    /**
     * 保持可读可写
     */
    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    /*---------------------后面是未实现的方法------------------------------*/

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException("setSavepoint");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException("setSavepoint");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("rollback");

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("releaseSavepoint");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new RuntimeException("not support exception");
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new RuntimeException("not support exception");
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLException("not support exception");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException("getTypeMap");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("setTypeMap");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new UnsupportedOperationException("nativeSQL");
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new UnsupportedOperationException("setCatalog");
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new UnsupportedOperationException("getCatalog");
    }

    public ConnectionHolder getConnectionHolder() {
        return this.connectionHolder;
    }

    @Override
    public void kill() throws SQLException {
        if (closed) {
            return;
        }

        List<SQLException> exceptions = new LinkedList<SQLException>();

        this.connectionHolder.kill();

        try {
            this.close();
        } catch (SQLException e) {
            exceptions.add(e);
        }

        ExceptionUtils.throwSQLException(exceptions, "kill tconnection", Collections.EMPTY_LIST);

    }

    @Override
    public void cancleQuery() throws SQLException {
        if (closed) {
            return;
        }

        List<SQLException> exceptions = new LinkedList<SQLException>();

        if (this.executionContext != null) {
            this.executionContext.cleanTempTables();
        }

        this.connectionHolder.cancel();

        try {
            // 关闭statement
            for (TStatement stmt : openedStatements) {
                try {
                    stmt.close(false);
                } catch (SQLException e) {
                    exceptions.add(e);
                }
            }
        } finally {
            openedStatements.clear();
        }

        ExceptionUtils.throwSQLException(exceptions, "cancleQuery tconnection", Collections.EMPTY_LIST);

    }

    @Override
    public long getLastInsertId() {
        return this.lastInsertId;
    }

    @Override
    public void setLastInsertId(long id) {
        this.lastInsertId = id;

    }
		@Override
	public void setSchema(String schema) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getSchema() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}
}
