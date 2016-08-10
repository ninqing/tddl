package com.taobao.tddl.optimizer.core.ast.dal;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.dml.ShowWithoutTable;

public class ShowWithoutTableNode extends BaseShowNode {

    public ShowWithoutTableNode(){
        super();
    }

    @Override
    public void build() {
    }

    @Override
    public IDataNodeExecutor toDataNodeExecutor(int shareIndex) {
        ShowWithoutTable show = new ShowWithoutTable();
        show.setType(type);
        show.executeOn(this.getDataNode());
        return show;
    }

    @Override
    public void assignment(Parameters parameterSettings) {

    }

    @Override
    public boolean isNeedBuild() {
        return false;
    }

    @Override
    public String toString(int inden, int shareIndex) {
        return "SHOW " + this.type.name();
    }

    @Override
    public IFunction getNextSubqueryOnFilter() {
        return null;
    }

    @Override
    public ShowWithoutTableNode copy() {
        return deepCopy();
    }

    @Override
    public ShowWithoutTableNode copySelf() {
        return deepCopy();
    }

    @Override
    public ShowWithoutTableNode deepCopy() {
        ShowWithoutTableNode node = new ShowWithoutTableNode();
        node.setType(this.type);
        node.executeOn(this.getDataNode());
        return node;
    }

}
