package com.taobao.tddl.executor.function.scalar.control;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

/**
 * If expr1 is TRUE (expr1 <> 0 and expr1 <> NULL) then IF() returns expr2;
 * otherwise it returns expr3. IF() returns a numeric or string value, depending
 * on the context in which it is used.
 * 
 * <pre>
 * mysql> SELECT IF(1>2,2,3);
 *         -> 3
 * mysql> SELECT IF(1<2,'yes','no');
 *         -> 'yes'
 * mysql> SELECT IF(STRCMP('test','test1'),'no','yes');
 *         -> 'no'
 * </pre>
 * 
 * If only one of expr2 or expr3 is explicitly NULL, the result type of the IF()
 * function is the type of the non-NULL expression.
 * 
 * @author jianghang 2014-4-15 上午10:57:54
 * @since 5.0.7
 */
public class If extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args[0] == null) {
            return args[2];
        }

        Long f = DataType.LongType.convertFrom(args[0]);
        if (f != 0) {
            return args[1];
        } else {
            return args[2];
        }
    }

    @Override
    public DataType getReturnType() {
        DataType type = null;
        if (function.getArgs().get(1) instanceof ISelectable) {
            type = ((ISelectable) function.getArgs().get(1)).getDataType();
        }
        if (type == null) {
            type = DataTypeUtil.getTypeOfObject(function.getArgs().get(1));
        }
        return type;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "IF" };
    }
}
