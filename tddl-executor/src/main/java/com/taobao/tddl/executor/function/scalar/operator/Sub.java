package com.taobao.tddl.executor.function.scalar.operator;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.Calculator;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DateType;
import com.taobao.tddl.optimizer.core.datatype.IntervalType;
import com.taobao.tddl.optimizer.core.datatype.TimestampType;

/**
 * @since 5.0.0
 */
public class Sub extends ScalarFunction {

    public Object computeInner(Object[] args) {
        DataType type = this.getReturnType();
        Calculator cal = type.getCalculator();
        // 如果加法中有出现IntervalType类型，转到时间函数的加法处理
        for (Object arg : args) {
            if (arg instanceof IntervalType && !(type instanceof TimestampType || type instanceof DateType)) {
                cal = DataType.TimestampType.getCalculator();
            }
        }

        if (type.getCalculator() != cal) {
            return type.convertFrom(cal.sub(args[0], args[1]));
        } else {
            return cal.sub(args[0], args[1]);
        }
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return this.computeInner(args);
    }

    @Override
    public DataType getReturnType() {
        return getFirstArgType();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "SUB", "-" };
    }
}
