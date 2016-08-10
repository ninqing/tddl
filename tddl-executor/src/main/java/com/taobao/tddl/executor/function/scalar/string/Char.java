package com.taobao.tddl.executor.function.scalar.string;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.function.ScalarFunction;
import com.taobao.tddl.executor.utils.ExecUtils;
import com.taobao.tddl.optimizer.core.datatype.DataType;

/**
 * http://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_char
 * 
 * @author jianghang 2014-4-9 下午6:48:23
 * @since 5.0.7
 */
public class Char extends ScalarFunction {

    @Override
    public DataType getReturnType() {
        return DataType.StringType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] { "CHAR" };
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        List<Character> chs = new ArrayList<Character>();
        for (Object obj : args) {
            if (ExecUtils.isNull(obj)) {
                Long data = DataType.LongType.convertFrom(obj);
                appendChars(data, chs);
            }
        }

        if (chs.size() == 0) {
            return null;
        }

        char[] chars = new char[chs.size()];
        int size = chs.size();
        for (int i = size - 1; i >= 0; i--) {
            chars[size - 1 - i] = chs.get(i);
        }

        return String.valueOf(chars);
    }

    public void appendChars(Long data, List<Character> chs) {
        while (true) {
            Character ch = Character.valueOf((char) (data % 256));
            chs.add(ch);

            if ((data >>= 8) == 0) {
                break;
            }
        }
    }

}
