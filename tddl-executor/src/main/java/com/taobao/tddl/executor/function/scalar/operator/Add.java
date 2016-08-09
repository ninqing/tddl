package com.taobao.tddl.executor.function.scalar.operator;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.Calculator;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.datatype.DateType;
import com.taobao.tddl.optimizer.core.datatype.IntervalType;
import com.taobao.tddl.optimizer.core.datatype.TimestampType;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * @since 5.0.0
 */
public class Add extends ScalarFunction {

    private Object computeInner(Object[] args) {
        DataType type = this.getReturnType();
        Calculator cal = type.getCalculator();
        // 如果加法中有出现IntervalType类型，转到时间函数的加法处理
        for (Object arg : args) {
            if (arg instanceof IntervalType && !(type instanceof TimestampType || type instanceof DateType)) {
                cal = DataType.TimestampType.getCalculator();
            }
        }

        if (type.getCalculator() != cal) {
            return type.convertFrom(cal.add(args[0], args[1]));
        } else {
            return cal.add(args[0], args[1]);
        }
    }

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

    @Override
    public String[] getFunctionNames() {
        return new String[] { "ADD", "+" };
    }

}
