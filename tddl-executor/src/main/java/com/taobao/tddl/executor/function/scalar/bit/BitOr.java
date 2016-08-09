package com.taobao.tddl.executor.function.scalar.bit;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * 对应mysql的bitor '|'
 * 
 * @author jianghang 2014-2-13 上午11:55:29
 * @since 5.0.0
 */
public class BitOr extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return computeInner(args);
    }

    private Object computeInner(Object[] args) {
        DataType type = this.getReturnType();
        return type.getCalculator().bitOr(args[0], args[1]);
    }

    @Override
    public DataType getReturnType() {
        DataType type = null;
        if (function.getArgs().get(0) instanceof ISelectable) {
            type = ((ISelectable) function.getArgs().get(0)).getDataType();
        }
        if (type == null) {
            type = DataTypeUtil.getTypeOfObject(function.getArgs().get(0));
        }

        if (type == DataType.BigIntegerType || type == DataType.BigDecimalType) {
            return DataType.BigIntegerType;
        } else {
            return DataType.LongType;
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "|" };
    }
}
