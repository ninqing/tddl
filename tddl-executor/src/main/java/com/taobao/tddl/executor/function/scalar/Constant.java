package com.taobao.tddl.executor.function.scalar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * 对应select 1中的1常量
 * 
 * @author jianghang 2014-4-15 上午11:33:08
 * @since 5.0.7
 */
public class Constant extends ScalarFunction {

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
        return new String[] { "CONSTANT" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) throws FunctionException {
        return args[0];
    }

}
