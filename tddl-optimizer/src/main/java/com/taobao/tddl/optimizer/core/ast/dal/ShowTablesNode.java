package com.taobao.tddl.optimizer.core.ast.dal;

import org.apache.commons.lang.StringUtils;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.bean.ShowTables;

/**
 * show tables node
 * 
 * @author jianghang 2014-5-13 下午11:18:00
 * @since 5.1.0
 */
public class ShowTablesNode extends ASTNode {

    private boolean full = false;
    private String  schema;
    private String  like;
    private String  where;

    public ShowTablesNode(){
    }

    public ShowTablesNode(boolean full, String schema, String like, String where){
        this.full = full;
        this.schema = schema;
        this.like = like;
        this.where = where;
    }

    @Override
    public void build() {
    }

    @Override
    public IDataNodeExecutor toDataNodeExecutor(int shareIndex) {
        ShowTables show = new ShowTables(full, schema, like, where);
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
    public IFunction getNextSubqueryOnFilter() {
        return null;
    }

    @Override
    public ShowTablesNode copy() {
        return deepCopy();
    }

    @Override
    public ShowTablesNode copySelf() {
        return deepCopy();
    }

    @Override
    public ShowTablesNode deepCopy() {
        ShowTablesNode node = new ShowTablesNode(full, schema, like, where);
        node.executeOn(this.getDataNode());
        return node;
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

}
