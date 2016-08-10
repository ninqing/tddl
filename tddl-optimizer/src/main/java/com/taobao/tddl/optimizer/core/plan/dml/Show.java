package com.taobao.tddl.optimizer.core.plan.dml;

import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.ast.dal.BaseShowNode.ShowType;
import com.taobao.tddl.optimizer.core.expression.IFilter;
import com.taobao.tddl.optimizer.core.plan.bean.DataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.query.IShow;

public abstract class Show extends DataNodeExecutor<IShow> implements IShow {

    protected ShowType type;
    private IFilter    filter;

    @Override
    public ShowType getType() {
        return type;
    }

    @Override
    public void setType(ShowType type) {
        this.type = type;
    }

    @Override
    public void accept(PlanVisitor visitor) {

    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

    @Override
    public void setWhereFilter(IFilter filter) {
        this.filter = filter;
    }

    @Override
    public IFilter getWhereFilter() {
        return this.filter;
    }

}
