package com.taobao.tddl.executor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.commons.lang.exception.ExceptionUtils;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.properties.ConnectionProperties;
import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.common.AtomicNumberCreator;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.common.ExecutorContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.ResultCursor;
import com.taobao.tddl.executor.cursor.impl.QueryPlanResultCursor;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.monitor.Monitor;
import com.taobao.tddl.optimizer.Optimizer;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IQueryTree;
import com.taobao.tddl.optimizer.exceptions.EmptyResultFilterException;

public class MatrixExecutor extends AbstractLifecycle implements IExecutor {

    private final static Logger       logger  = LoggerFactory.getLogger(MatrixExecutor.class);
    private static final String       EXPLAIN = "explain";
    /**
     * id 生成器
     */
    private final AtomicNumberCreator idGen   = AtomicNumberCreator.getNewInstance();

    /**
     * client端核心流程 解析 优化 执行
     */
    public ResultCursor execute(String sql, ExecutionContext executionContext) throws TddlException {
        // client端核心流程
        try {
            int explainIndex = explainIndex(sql);
            if (explainIndex > 0) {
                sql = sql.substring(explainIndex);
            }
            IDataNodeExecutor qc = parseAndOptimize(sql, executionContext);
            if (explainIndex > 0) {
                return createQueryPlanResultCursor(qc);
            }

            return execByExecPlanNode(qc, executionContext);
        } catch (EmptyResultFilterException e) {
            return new ResultCursor.EmptyResultCursor(executionContext);
        } catch (Exception e) {
            if (ExceptionUtils.getCause(e) instanceof EmptyResultFilterException) {
                return new ResultCursor.EmptyResultCursor(executionContext);
            } else {
                throw new TddlException(e);
            }
        }
    }

    private ResultCursor createQueryPlanResultCursor(IDataNodeExecutor qc) {
        return new QueryPlanResultCursor(qc.toString(), null);
    }

    private int explainIndex(String sql) {
        String temp = sql;
        int i = 0;
        for (; i < temp.length(); ++i) {
            switch (temp.charAt(i)) {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    continue;
            }
            break;
        }

        if (temp.toLowerCase().startsWith(EXPLAIN, i)) {
            return i + EXPLAIN.length();
        } else {
            return -1;
        }
    }

    public IDataNodeExecutor parseAndOptimize(String sql, ExecutionContext executionContext) throws TddlException {
        boolean cache = GeneralUtil.getExtraCmdBoolean(executionContext.getExtraCmds(),
            ConnectionProperties.OPTIMIZER_CACHE,
            true);

        Optimizer op = OptimizerContext.getContext().getOptimizer();

        Object astOrHint = op.optimizeAstOrHint(sql,
            executionContext.getParams(),
            cache,
            executionContext.getExtraCmds());

        if (astOrHint instanceof IDataNodeExecutor) {
            return (IDataNodeExecutor) astOrHint;
        }

        ASTNode ast = (ASTNode) astOrHint;
        boolean existSubquery = false;
        IFunction subQueryFunction = ast.getNextSubqueryOnFilter();
        while (true) {
            if (subQueryFunction == null) {
                break;
            }

            existSubquery = true;
            QueryTreeNode query = (QueryTreeNode) subQueryFunction.getArgs().get(0);
            IDataNodeExecutor subQueryPlan = op.optimizePlan(query,
                executionContext.getParams(),
                executionContext.getExtraCmds());

            subQueryFunction.getArgs().set(0, subQueryPlan);

            Object subQueryResult = ((ScalarFunction) subQueryFunction.getExtraFunction()).scalarCalucate((IRowSet) null,
                executionContext);

            Map<Long, Object> result = new HashMap();
            result.put(query.getSubqueryOnFilterId(), subQueryResult);
            subQueryFunction = op.assignmentSubquery(ast, result, executionContext.getExtraCmds());

        }

        if (existSubquery) {// 如果存在子查询,替换子查询数据后需要重新构建下语法树,子查询会移动为subqueryFilter
            ast = op.optimizeAst(ast, executionContext.getParams(), executionContext.getExtraCmds());
        }

        IDataNodeExecutor queryPlan = op.optimizePlan(ast,
            executionContext.getParams(),
            executionContext.getExtraCmds());
        return queryPlan;

    }

    @Override
    public ResultCursor execByExecPlanNode(IDataNodeExecutor qc, ExecutionContext executionContext)
                                                                                                   throws TddlException {
        if (logger.isDebugEnabled()) {
            logger.warn("extraCmd:\n" + executionContext.getExtraCmds());
            logger.warn("ParameterContext:\n" + executionContext.getParams());
        }

        // client端核心流程y
        try {
            long time = System.currentTimeMillis();
            ISchematicCursor sc = ExecutorContext.getContext()
                .getTopologyExecutor()
                .execByExecPlanNode(qc, executionContext);
            ResultCursor rc = this.wrapResultCursor(qc, sc, executionContext);
            // 控制语句
            time = Monitor.monitorAndRenewTime(Monitor.KEY1, Monitor.KEY2_TDDL_EXECUTE, Monitor.Key3Success, time);
            if (qc instanceof IQueryTree) {
                // 做下表名替换
                List columnsForResultSet = ((IQueryTree) qc).getColumns();
                if (((IQueryTree) qc).getAlias() != null) {
                    columnsForResultSet = ExecUtils.copySelectables(columnsForResultSet);
                    for (Object s : columnsForResultSet) {
                        ((ISelectable) s).setTableName(((IQueryTree) qc).getAlias());
                    }
                }
                rc.setOriginalSelectColumns(columnsForResultSet);
            }
            return rc;
        } catch (EmptyResultFilterException e) {
            return new ResultCursor.EmptyResultCursor(executionContext);
        } catch (Exception e) {
            if (ExceptionUtils.getCause(e) instanceof EmptyResultFilterException) {
                return new ResultCursor.EmptyResultCursor(executionContext);
            } else {
                throw new TddlException(e);
            }
        }
    }

    @Override
    public Future<List<ISchematicCursor>> execByExecPlanNodesFuture(List<IDataNodeExecutor> qcs,
                                                                    ExecutionContext executionContext)
                                                                                                      throws TddlException {
        return ExecutorContext.getContext().getTopologyExecutor().execByExecPlanNodesFuture(qcs, executionContext);
    }

    @Override
    public Future<ISchematicCursor> execByExecPlanNodeFuture(IDataNodeExecutor qc, ExecutionContext executionContext)
                                                                                                                     throws TddlException {
        return ExecutorContext.getContext().getTopologyExecutor().execByExecPlanNodeFuture(qc, executionContext);
    }

    @Override
    public ResultCursor commit(ExecutionContext executionContext) throws TddlException {
        return ExecutorContext.getContext().getTopologyExecutor().commit(executionContext);
    }

    @Override
    public ResultCursor rollback(ExecutionContext executionContext) throws TddlException {
        return ExecutorContext.getContext().getTopologyExecutor().rollback(executionContext);

    }

    @Override
    public Future<ResultCursor> commitFuture(ExecutionContext executionContext) throws TddlException {
        return ExecutorContext.getContext().getTopologyExecutor().commitFuture(executionContext);
    }

    @Override
    public Future<ResultCursor> rollbackFuture(ExecutionContext executionContext) throws TddlException {
        return ExecutorContext.getContext().getTopologyExecutor().rollbackFuture(executionContext);
    }

    protected ResultCursor wrapResultCursor(IDataNodeExecutor command, ISchematicCursor iSchematicCursor,
                                            ExecutionContext context) throws TddlException {
        ResultCursor cursor;
        // 包装为可以传输的ResultCursor
        if (command instanceof IQueryTree) {
            if (!(iSchematicCursor instanceof ResultCursor)) {
                cursor = new ResultCursor(iSchematicCursor, context);
            } else {
                cursor = (ResultCursor) iSchematicCursor;
            }

        } else {
            if (!(iSchematicCursor instanceof ResultCursor)) {
                cursor = new ResultCursor(iSchematicCursor, context);
            } else {
                cursor = (ResultCursor) iSchematicCursor;
            }
        }
        generateResultIdAndPutIntoResultSetMap(cursor);
        return cursor;
    }

    private void generateResultIdAndPutIntoResultSetMap(ResultCursor cursor) {
        int id = idGen.getIntegerNextNumber();
        cursor.setResultID(id);
    }

}
