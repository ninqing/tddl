package com.taobao.tddl.executor.common;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.jdbc.IConnection;
import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.common.properties.ParamManager;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.repo.RepositoryDefault;
import com.taobao.tddl.executor.spi.IRepository;
import com.taobao.tddl.executor.spi.ITable;
import com.taobao.tddl.executor.spi.ITempTable;
import com.taobao.tddl.executor.spi.ITransaction;
import com.taobao.tddl.executor.spi.ITransaction.DistributedTransaction;

/**
 * 一次执行过程中的上下文
 * 
 * @author whisper
 */
public class ExecutionContext {

    private static final Logger                logger                      = LoggerFactory.getLogger(ExecutionContext.class);

    /**
     * 当前事务的执行group，只支持单group的事务
     */
    private String                             transactionGroup            = null;

    /**
     * 当前运行时的存储对象
     */
    private IRepository                        currentRepository           = new RepositoryDefault();

    /**
     * 是否自动关闭结果集。目前这个东西已经基本无效。除了在update等查询中有使用
     */
    private boolean                            closeResultSet;
    /**
     * 当前事务
     */
    private ITransaction                       transaction;

    private Map<String, Object>                extraCmds                   = new HashMap();

    private ParamManager                       paramManager                = new ParamManager(extraCmds);
    private Parameters                         params                      = null;

    private String                             isolation                   = null;

    private DistributedTransaction             distributedTransactionConfg = DistributedTransaction.DEFAULT;

    private ExecutorService                    concurrentService;

    private boolean                            autoCommit                  = true;

    private int                                txIsolation                 = -1;

    private String                             groupHint                   = null;

    private int                                autoGeneratedKeys           = -1;

    private int[]                              columnIndexes               = null;

    private String[]                           columnNames                 = null;

    private int                                resultSetType               = -1;

    private int                                resultSetConcurrency        = -1;

    private int                                resultSetHoldability        = -1;

    private ConnectionHolder                   connectionHolder;

    volatile private Cache<String, ITempTable> tempTables                  = CacheBuilder.newBuilder().build();

    IConnection                                connection                  = null;

    private InputStream                        localInFileInputStream      = null;

    public Cache<String, ITempTable> getTempTables() {
        return tempTables;
    }

    public void setTempTables(Cache<String, ITempTable> tempTables) {
        this.tempTables = tempTables;
    }

    public ExecutionContext(){

    }

    public IRepository getCurrentRepository() {
        return currentRepository;
    }

    public void setCurrentRepository(IRepository currentRepository) {
        this.currentRepository = currentRepository;
    }

    public boolean isCloseResultSet() {
        return closeResultSet;
    }

    public void setCloseResultSet(boolean closeResultSet) {
        this.closeResultSet = closeResultSet;
    }

    public ITransaction getTransaction() {
        return transaction;
    }

    public void setTransaction(ITransaction transaction) {
        this.transaction = transaction;

    }

    public Map<String, Object> getExtraCmds() {
        return extraCmds;
    }

    public void setExtraCmds(Map<String, Object> extraCmds) {
        this.extraCmds = extraCmds;

        this.paramManager = new ParamManager(extraCmds);
    }

    public Parameters getParams() {
        return params;
    }

    public void setParams(Parameters params) {
        this.params = params;
    }

    public String getIsolation() {
        return isolation;
    }

    public void setIsolation(String isolation) {
        this.isolation = isolation;
    }

    public ExecutorService getExecutorService() {
        return this.concurrentService;
    }

    public void setExecutorService(ExecutorService concurrentService) {
        this.concurrentService = concurrentService;
    }

    public String getTransactionGroup() {
        return transactionGroup;
    }

    public void setTransactionGroup(String transactionGroup) {
        this.transactionGroup = transactionGroup;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
        if (this.getTransaction() != null) {
            this.getTransaction().setAutoCommit(autoCommit);
        }
    }

    public String getGroupHint() {
        return groupHint;
    }

    public void setGroupHint(String groupHint) {
        this.groupHint = groupHint;
    }

    public ConnectionHolder getConnectionHolder() {
        return this.connectionHolder;
    }

    public void setConnectionHolder(ConnectionHolder ch) {
        this.connectionHolder = ch;
    }

    public int getAutoGeneratedKeys() {
        return autoGeneratedKeys;
    }

    public void setAutoGeneratedKeys(int autoGeneratedKeys) {
        this.autoGeneratedKeys = autoGeneratedKeys;
    }

    public int[] getColumnIndexes() {
        return columnIndexes;
    }

    public void setColumnIndexes(int[] columnIndexes) {
        this.columnIndexes = columnIndexes;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public int getResultSetType() {
        return resultSetType;
    }

    public void setResultSetType(int resultSetType) {
        this.resultSetType = resultSetType;
    }

    public int getResultSetConcurrency() {
        return resultSetConcurrency;
    }

    public void setResultSetConcurrency(int resultSetConcurrency) {
        this.resultSetConcurrency = resultSetConcurrency;
    }

    public int getResultSetHoldability() {
        return resultSetHoldability;
    }

    public void setResultSetHoldability(int resultSetHoldability) {
        this.resultSetHoldability = resultSetHoldability;
    }

    public ParamManager getParamManager() {
        return this.paramManager;
    }

    public void setParamManager(ParamManager pm) {
        this.paramManager = pm;
    }

    public void cleanTempTables() {
        for (ITable tempTable : getTempTables().asMap().values()) {
            try {
                tempTable.close();
            } catch (TddlException e) {
                logger.warn("temp table close failed", e);
            }
        }

    }

    public IConnection getConnection() {
        return connection;
    }

    public void setConnection(IConnection connection) {
        this.connection = connection;
    }

    public DistributedTransaction getDistributedTransactionConfg() {
        return distributedTransactionConfg;
    }

    public void setDistributedTransactionConfg(DistributedTransaction distributedTransactionConfg) {
        this.distributedTransactionConfg = distributedTransactionConfg;
    }

    public int getTxIsolation() {
        return txIsolation;
    }

    public void setTxIsolation(int txIsolation) {
        this.txIsolation = txIsolation;
    }

    public void setLocalInfileInputStream(InputStream stream) {
        this.localInFileInputStream = stream;
    }

    public InputStream getLocalInfileInputStream() {
        return this.localInFileInputStream;
    }

}
