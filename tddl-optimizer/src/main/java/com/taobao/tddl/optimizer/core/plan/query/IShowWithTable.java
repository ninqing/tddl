package com.taobao.tddl.optimizer.core.plan.query;

public interface IShowWithTable extends IShow {

    void setTableName(String tableName);

    String getTableName();

}
