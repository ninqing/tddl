package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * Returns the sign of the argument as -1, 0, or 1, depending on whether X is
 * negative, zero, or positive.
 * 
 * <pre>
 * mysql> SELECT SIGN(-32);
 *         -> -1
 * mysql> SELECT SIGN(0);
 *         -> 0
 * mysql> SELECT SIGN(234);
 *         -> 1
 * </pre>
 * 
 * @author jianghang 2014-4-14 下午10:50:20
 * @since 5.0.7
 */
public class Sign extends ScalarFunction {

    private Object computeInner(Object[] args) {
        DataType type = this.getReturnType();
        Object arg = type.convertFrom(args[0]);
        Object zero = type.convertFrom(0);

        if (arg instanceof Comparable) {
            return type.convertFrom((((Comparable) arg).compareTo(zero)));
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
        return new String[] { "SIGN" };
    }

}
