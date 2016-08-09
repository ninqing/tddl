package com.taobao.tddl.executor.function.scalar.datatime;

import java.util.Calendar;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.datatype.DataTypeUtil;
import com.taobao.tddl.optimizer.core.datatype.IntervalType;
import com.taobao.tddl.optimizer.core.expression.ISelectable;

public class DateSub extends ScalarFunction {

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (ExecUtils.isNull(arg)) {
                return null;
            }
        }

        java.sql.Timestamp timestamp = DataType.TimestampType.convertFrom(args[0]);
        Calendar cal = Calendar.getInstance();
        cal.setTime(timestamp);

        Object day = args[1];
        if (day instanceof IntervalType) {
            ((IntervalType) day).process(cal, -1);
        } else {
            cal.add(Calendar.DAY_OF_YEAR, DataType.IntegerType.convertFrom(day));
        }

        DataType type = getReturnType();
        return type.convertFrom(cal);
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
        return new String[] { "DATE_SUB", "SUBDATE" };
    }
}
