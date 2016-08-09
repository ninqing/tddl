package com.taobao.tddl.executor.function.scalar.math;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * Modulo operation. Returns the remainder of N divided by M.
 * 
 * <pre>
 * mysql> SELECT MOD(234, 10);
 *         -> 4
 * mysql> SELECT 253 % 7;
 *         -> 1
 * mysql> SELECT MOD(29,9);
 *         -> 2
 * mysql> SELECT 29 MOD 9;
 *         -> 2
 * </pre>
 * 
 * This function is safe to use with BIGINT values. MOD() also works on values
 * that have a fractional part and returns the exact remainder after division:
 * 
 * <pre>
 * mysql> SELECT MOD(34.5,3);
 *         -> 1.5
 * </pre>
 * 
 * MOD(N,0) returns NULL.
 */
public class Mod extends ScalarFunction {

    private Object computeInner(Object[] args) {
        DataType type = this.getReturnType();
        return type.getCalculator().mod(args[0], args[1]);

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
        return new String[] { "MOD", "%" };
    }
}
