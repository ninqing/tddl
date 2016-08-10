package com.taobao.tddl.optimizer.costbased;

import java.util.List;
import java.util.Map;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.QueryTreeNode;
import com.taobao.tddl.optimizer.core.ast.dml.InsertNode;
import com.taobao.tddl.optimizer.core.ast.dml.UpdateNode;
import com.taobao.tddl.optimizer.core.expression.IBooleanFilter;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.expression.IFilter.OPERATION;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ILogicalFilter;
import com.taobao.tddl.optimizer.core.expression.IOrderBy;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.ISequenceVal;
import com.taobao.tddl.optimizer.exception.OptimizerException;

/**
 * sqeuence的处理类
 * 
 * <pre>
 * 1. 遍历下所有的节点，设置sequence.nextval的下标
 * 
 * <pre>
 * @author jianghang 2014-5-5 上午11:38:30
 * @since 5.1.0
 */
public class SequencePreProcessor {

    public static ASTNode opitmize(ASTNode node, Parameters parameters, Map<String, Object> extraCmd)
                                                                                                     throws OptimizerException {
        if (node instanceof DMLNode) {
            findDML((DMLNode) node, parameters);
        } else {
            findQuery((QueryTreeNode) node, parameters);
        }
        return node;
    }

    private static void findDML(DMLNode node, Parameters parameters) {
        if (node.isMultiValues()) {
            if (node.getMultiValues() != null) {
                for (Object objs : node.getMultiValues()) {
                    for (Object obj : (List) objs) {
                        findObject(obj, parameters);
                    }
                }
            }
        } else {
            if (node.getValues() != null) {
                for (Object obj : node.getValues()) {
                    findObject(obj, parameters);
                }
            }
        }

        if (node instanceof InsertNode) {
            InsertNode insert = (InsertNode) node;
            if (insert.getDuplicateUpdateValues() != null) {
                for (Object obj : insert.getDuplicateUpdateValues()) {
                    findObject(obj, parameters);
                }
            }
        } else if (node instanceof UpdateNode) {
            UpdateNode update = (UpdateNode) node;
            if (update.getUpdateValues() != null) {
                for (Object obj : update.getUpdateValues()) {
                    findObject(obj, parameters);
                }
            }
        }
    }

    private static void findQuery(QueryTreeNode qtn, Parameters parameters) {
        findFilter(qtn.getKeyFilter(), parameters);
        findFilter(qtn.getWhereFilter(), parameters);
        findFilter(qtn.getResultFilter(), parameters);
        findFilter(qtn.getOtherJoinOnFilter(), parameters);
        findFilter(qtn.getHavingFilter(), parameters);

        for (ISelectable select : qtn.getColumnsSelected()) {
            // 可能替换了subquery
            findSelectable(select, parameters);
        }

        if (qtn.getOrderBys() != null) {
            for (IOrderBy orderBy : qtn.getOrderBys()) {
                findOrderBy(orderBy, parameters);
            }
        }

        if (qtn.getGroupBys() != null) {
            for (IOrderBy groupBy : qtn.getGroupBys()) {
                findOrderBy(groupBy, parameters);
            }
        }
    }

    private static void findFilter(IFilter filter, Parameters parameters) {
        if (filter == null) {
            return;
        }

        if (filter instanceof ILogicalFilter) {
            for (IFilter sub : ((ILogicalFilter) filter).getSubFilter()) {
                findFilter(sub, parameters);
            }
        } else {
            findBooleanFilter((IBooleanFilter) filter, parameters);
        }

    }

    private static void findBooleanFilter(IBooleanFilter filter, Parameters parameters) {
        if (filter == null) {
            return;
        }

        findObject(filter.getColumn(), parameters);
        findObject(filter.getValue(), parameters);
        if (filter.getOperation() == OPERATION.IN) {
            List<Object> values = filter.getValues();
            if (values != null && !values.isEmpty()) {
                for (int i = 0; i < values.size(); i++) {
                    findObject(values.get(i), parameters);
                }
            }
        }

    }

    private static void findObject(Object obj, Parameters parameters) {
        if (obj instanceof ISelectable) {
            findSelectable((ISelectable) obj, parameters);
        } else if (obj instanceof ISequenceVal) {
            findSequenceVal((ISequenceVal) obj, parameters);
        } else if (obj instanceof QueryTreeNode) { // scalar subquery
            // 深度优先,尝试递归找一下
            findQuery((QueryTreeNode) obj, parameters);
        }
    }

    private static void findSelectable(ISelectable select, Parameters parameters) {
        if (select instanceof IBooleanFilter) {
            findBooleanFilter((IBooleanFilter) select, parameters);
        } else if (select instanceof ILogicalFilter) {
            findFilter((IFilter) select, parameters);
        } else if (select instanceof IFunction) {
            findFunction((IFunction) select, parameters);
        } else if (select instanceof IColumn) {
            findColumn((IColumn) select, parameters);
        }
    }

    private static void findColumn(IColumn column, Parameters parameters) {
        // do nothing
    }

    private static void findOrderBy(IOrderBy order, Parameters parameters) {
        if (order.getColumn() instanceof ISelectable) {
            findSelectable(order.getColumn(), parameters);
        }
    }

    private static void findFunction(IFunction f, Parameters parameters) {
        for (Object arg : f.getArgs()) {
            findObject(arg, parameters);
        }
    }

    private static void findSequenceVal(ISequenceVal s, Parameters parameters) {
        int index = parameters.getFirstParameter().size() + parameters.getSequenceSize().incrementAndGet();
        ((ISequenceVal) s).setOriginIndex(index);
    }

}
