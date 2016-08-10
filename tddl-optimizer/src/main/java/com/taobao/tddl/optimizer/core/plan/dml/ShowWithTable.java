package com.taobao.tddl.optimizer.core.plan.dml;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;

import com.taobao.tddl.optimizer.core.plan.query.IShowWithTable;
import com.taobao.tddl.optimizer.utils.OptimizerToString;

public class ShowWithTable extends Show implements IShowWithTable {

    private String tableName;

    @Override
    public String toStringWithInden(int inden) {

        String tabTittle = OptimizerToString.getTab(inden);
        String tabContent = OptimizerToString.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();
        appendln(sb, tabTittle + "SHOW " + this.type + " FROM " + this.tableName);
        appendField(sb, "executeOn", this.getDataNode(), tabContent);

        return sb.toString();
    }

    @Override
    public ShowWithTable copy() {
        ShowWithTable newShow = new ShowWithTable();
        newShow.setTableName(this.tableName);
        newShow.setType(this.type);
        newShow.setSql(this.sql);
        newShow.executeOn(this.getDataNode());

        return newShow;
    }

    @Override
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String getTableName() {
        return this.tableName;
    }

}
