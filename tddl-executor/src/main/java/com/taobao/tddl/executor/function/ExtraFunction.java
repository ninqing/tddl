package com.taobao.tddl.executor.function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.rowset.IRowSet;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.expression.IColumn;
import com.taobao.tddl.optimizer.core.expression.IExtraFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.IFunction.FunctionType;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

public abstract class ExtraFunction implements IExtraFunction {

    protected static final Logger logger = LoggerFactory.getLogger(ExtraFunction.class);
    protected IFunction           function;

    @Override
    public void setFunction(IFunction function) {
        this.function = function;
    }

    /**
     * 如果可以用db的函数，那就直接使用
     * 
     * @param function
     */
    protected abstract String getDbFunction();

    protected List getReduceArgs(IFunction func) {
        String resArgs = getDbFunction();
        Object[] obs = resArgs.split(",");
        return Arrays.asList(obs);
    }

    protected List getMapArgs(IFunction func) {
        return func.getArgs();
    }

    protected Object getArgValue(Object funcArg, IRowSet kvPair, ExecutionContext ec) {
        if (funcArg instanceof IFunction) {
            if (((IFunction) funcArg).getExtraFunction().getFunctionType().equals(FunctionType.Aggregate)) {
                // aggregate function
                return ExecUtils.getValueByIColumn(kvPair, ((ISelectable) funcArg));

            } else {
                // scalar function

                return ((ScalarFunction) ((IFunction) funcArg).getExtraFunction()).scalarCalucate(kvPair, ec);
            }
        } else if (funcArg instanceof ISelectable) {// 如果是IColumn，那么应该从输入的参数中获取对应column
            if (IColumn.STAR.equals(((ISelectable) funcArg).getColumnName())) {
                return kvPair;
            } else {
                return ExecUtils.getValueByIColumn(kvPair, ((ISelectable) funcArg));
            }
        } else if (funcArg instanceof List) {
            List newArg = new ArrayList(((List) funcArg).size());

            for (Object subArg : (List) funcArg) {
                newArg.add(this.getArgValue(subArg, kvPair, ec));
            }

            return newArg;
        } else {
            return funcArg;
        }
    }

}
