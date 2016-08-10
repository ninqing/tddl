package com.taobao.tddl.optimizer.core.plan.bean;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.optimizer.core.PlanVisitor;
import com.taobao.tddl.optimizer.core.plan.query.IShowTables;

public class ShowTables extends DataNodeExecutor<IShowTables> implements IShowTables {

    private boolean full;
    private String  schema;
    private String  like;
    private String  where;

    public ShowTables(boolean full, String schema, String like, String where){
        this.full = full;
        this.schema = schema;
        this.like = like;
        this.where = where;
    }

    public boolean isFull() {
        return full;
    }

    public String getSchema() {
        return schema;
    }

    public String getLike() {
        return like;
    }

    public String getWhere() {
        return where;
    }

    @Override
    public String toStringWithInden(int inden) {
        StringBuilder builder = new StringBuilder();
        builder.append("show");
        if (full) {
            builder.append(" full");
        }
        builder.append(" tables");
        if (StringUtils.isNotEmpty(schema)) {
            builder.append(" in ").append(schema);
        }
        if (StringUtils.isNotEmpty(like)) {
            builder.append(" like ").append(like);
        }
        if (StringUtils.isNotEmpty(where)) {
            builder.append(" where ").append(where);
        }
        return builder.toString();

    }

    @Override
    public IShowTables copy() {
        return new ShowTables(full, schema, like, where);
    }

    @Override
    public void accept(PlanVisitor visitor) {

    }

    @Override
    public String toString() {
        return toStringWithInden(0);
    }

}
