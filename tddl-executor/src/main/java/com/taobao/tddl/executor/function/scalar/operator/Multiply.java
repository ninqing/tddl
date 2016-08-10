package com.taobao.tddl.executor.function.scalar.operator;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;

public class Multiply extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return computeInner(args);
    }

    @Override
    public DataType getReturnType() {
        DataType type = getFirstArgType();
        if (type == DataType.IntegerType || type == DataType.ShortType) {
            return DataType.LongType;
        } else {
            return type;
        }
    }

    private Object computeInner(Object[] args) {
        DataType type = this.getReturnType();
        return type.getCalculator().multiply(args[0], args[1]);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "*", "MULTIPLY" };
    }
}
