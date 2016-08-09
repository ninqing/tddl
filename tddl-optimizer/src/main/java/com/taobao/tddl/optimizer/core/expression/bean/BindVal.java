package com.taobao.tddl.optimizer.core.expression.bean;

import com.taobao.tddl.common.exception.NotSupportException;
import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.expression.IBindVal;

/**
 * 绑定变量
 * 
 * @author Whisper
 */
public class BindVal implements IBindVal {

    private final int index;
    private Object    value;

    public BindVal(int index){
        this.index = index;
    }

    @Override
    public int compareTo(Object o) {
        throw new NotSupportException();
    }

    @Override
    public Object assignment(Parameters parameterSettings) {
        ParameterContext paramContext = parameterSettings.getCurrentParameter().get(index);
        if (paramContext == null) {
            throw new TddlRuntimeException("can't find param by index :" + index + " ." + "context : "
                                           + parameterSettings);
        }

        if (paramContext.getArgs()[1] == null) {
            value = NullValue.getNullValue();
        } else {
            value = paramContext.getArgs()[1];
        }

        if (parameterSettings.isBatch()) {
            // 针对batch，不做绑定变量替换
            return this;
        } else {
            return value;
        }
    }

    @Override
    public String toString() {
        return "BindVal [index=" + index + ", value=" + value + "]";
    }

    public int getBindVal() {
        return index;
    }

    @Override
    public void accept(PlanVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public Integer getOrignIndex() {
        return index;
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public IBindVal copy() {
        BindVal newBindVal = new BindVal(index);
        return newBindVal;
    }

}
