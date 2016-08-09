package com.taobao.tddl.executor.function.scalar.filter;

import java.util.Collection;
import java.util.List;

import com.taobao.tddl.common.utils.GeneralUtil;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * @since 5.0.0
 */
public class In extends Filter {

    @Override
    protected Boolean computeInner(Object[] args, ExecutionContext ec) {
        DataType type = this.getArgType();

        Object left = args[0];
        Object right = args[1];

        if (right instanceof List) {

            if (!GeneralUtil.isEmpty((Collection) right)) {
                if (((List) right).get(0) instanceof List) {
                    right = ((List) right).get(0);
                }
            }
            for (Object eachRight : (List) right) {
                if (type.compare(left, eachRight) == 0) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "IN" };
    }
}
