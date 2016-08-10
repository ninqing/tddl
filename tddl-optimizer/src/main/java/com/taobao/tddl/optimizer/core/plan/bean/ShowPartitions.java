package com.taobao.tddl.optimizer.core.plan.bean;

import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.plan.query.IShowPartitions;

public class ShowPartitions extends DataNodeExecutor<IShowPartitions> implements IShowPartitions {

    private String tableName;

    public ShowPartitions(String tableName){
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toStringWithInden(int inden) {
        return "show partitions from " + this.tableName;
    }

    @Override
    public IShowPartitions copy() {
        return new ShowPartitions(tableName);
    }

    @Override
    public void accept(PlanVisitor visitor) {

    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

}
