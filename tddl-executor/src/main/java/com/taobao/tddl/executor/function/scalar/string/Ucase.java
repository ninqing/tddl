package com.taobao.tddl.executor.function.scalar.string;

import com.taobao.tddl.common.utils.TStringUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.exceptions.FunctionException;

public class Ucase extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "UCASE" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) throws FunctionException {
        if (ExecUtils.isNull(args[0])) {
            return null;
        }

        String str = DataType.StringType.convertFrom(args[0]);

        return TStringUtil.upperCase(str);

    }
}
