package com.taobao.tddl.optimizer.core.ast.dal;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.bean.ShowPartitions;

public class ShowPartitionsNode extends ASTNode {

    private String tableName;

    public ShowPartitionsNode(String tableName){
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
        ShowPartitions show = new ShowPartitions(this.tableName);
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
        return "show partitions from " + this.tableName;
    }

    @Override
    public IFunction getNextSubqueryOnFilter() {
        return null;
    }

    @Override
    public ShowPartitionsNode copy() {
        return deepCopy();
    }

    @Override
    public ShowPartitionsNode copySelf() {
        return deepCopy();
    }

    @Override
    public ShowPartitionsNode deepCopy() {
        ShowPartitionsNode node = new ShowPartitionsNode(tableName);
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
