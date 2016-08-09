package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * Returns the absolute value of X.
 * 
 * <pre>
 * mysql> SELECT ABS(2);
 *         -> 2
 * mysql> SELECT ABS(-32);
 *         -> 32
 * </pre>
 * 
 * This function is safe to use with BIGINT values.
 * 
 * @author jianghang 2014-4-14 下午9:35:18
 * @since 5.0.7
 */
public class Abs extends ScalarFunction {

    private Object computeInner(Object[] args) {
        DataType type = this.getReturnType();
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        Object arg = type.convertFrom(args[0]);
        Object zero = type.convertFrom(0);

        if (arg instanceof Comparable) {
            if (((Comparable) arg).compareTo(zero) < 0) {
                return type.getCalculator().multiply(arg, -1);
            }

            return arg;
        } else {
            throw new TddlRuntimeException("不支持abs的类型计算:" + type);
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
        return new String[] { "ABS" };
    }

}
