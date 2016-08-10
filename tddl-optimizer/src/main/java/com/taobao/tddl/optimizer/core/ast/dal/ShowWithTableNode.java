package com.taobao.tddl.optimizer.core.ast.dal;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.dml.ShowWithTable;

public class ShowWithTableNode extends BaseShowNode {

    private String tableName;

    public ShowWithTableNode(String tableName){
        super();
        this.tableName = tableName;
    }

    @Override
    public void build() {
        if (tableName == null) {
            throw new IllegalArgumentException("tableName is null");
        }

        OptimizerContext.getContext().getSchemaManager().getTable(tableName);
    }

    @Override
    public IDataNodeExecutor toDataNodeExecutor(int shareIndex) {
        ShowWithTable show = new ShowWithTable();
        show.setType(type);
        show.setTableName(this.tableName);
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
        return "SHOW " + this.type.name() + " FROM " + this.tableName;
    }

    @Override
    public IFunction getNextSubqueryOnFilter() {
        return null;
    }

    @Override
    public ShowWithTableNode copy() {
        return deepCopy();
    }

    @Override
    public ShowWithTableNode copySelf() {
        return deepCopy();
    }

    @Override
    public ShowWithTableNode deepCopy() {
        ShowWithTableNode node = new ShowWithTableNode(tableName);
        node.setType(this.type);
        node.executeOn(this.getDataNode());
        return node;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

}
