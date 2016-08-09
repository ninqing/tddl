package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

/**
 * <pre>
 * ASCII(str)
 * 
 * Returns the numeric value of the leftmost character of the string str. Returns 0 if str is the empty string. Returns NULL if str is NULL. ASCII() works for 8-bit characters.
 * 
 * mysql> SELECT ASCII('2');
 *         -> 50
 * mysql> SELECT ASCII(2);
 *         -> 50
 * mysql> SELECT ASCII('dx');
 *         -> 100
 * 
 * See also the ORD() function.
 * </pre>
 * 
 * @author mengshi.sunmengshi 2014年4月11日 下午1:33:36
 * @since 5.1.0
 */
public class Ascii extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.IntegerType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "ASCII" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) throws FunctionException {
        Object arg = args[0];

        if (ExecUtils.isNull(arg)) {
            return null;
        }

        String str = DataType.StringType.convertFrom(arg);

        if (TStringUtil.isEmpty(str)) {
            return 0;
        }

        char leftMost = str.charAt(0);

        return (int) leftMost;
    }

}
