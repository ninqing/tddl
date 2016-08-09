package com.taobao.tddl.executor.function.scalar.operator;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * mysql的Minus函数,取负数操作
 * 
 * @author jianghang 2014-2-13 上午11:43:32
 * @since 5.0.0
 */
public class Minus extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return this.computeInner(args);
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
        return type;
    }

    private Object computeInner(Object[] args) {
        DataType type = getReturnType();
        // -min(id) = min(id) * -1
        return type.getCalculator().multiply(args[0], -1);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "MINUS" };
    }
}
