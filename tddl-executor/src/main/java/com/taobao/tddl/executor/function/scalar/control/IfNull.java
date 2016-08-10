package com.taobao.tddl.executor.function.scalar.control;

import java.util.List;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @since 5.0.0
 */
public class IfNull extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args == null) {
            return null;
        }

        if (ExecUtils.isNull(args[0])) {
            return args[1];
        } else {
            return args[0];
        }

    }

    @Override
    public DataType getReturnType() {
        DataType lastType = null;
        List args = function.getArgs();
        // 遍历所有的then字段的类型
        for (int i = 0; i < args.size(); i++) {
            DataType argType = getArgType(args.get(i));
            if (lastType == null) {
                lastType = argType;
            } else if (lastType != argType) {
                lastType = DataType.StringType;
            }
        }

        return lastType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "IFNULL" };
    }
}
