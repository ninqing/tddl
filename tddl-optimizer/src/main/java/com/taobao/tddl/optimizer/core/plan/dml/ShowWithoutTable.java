package com.taobao.tddl.optimizer.core.plan.dml;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;

import com.taobao.tddl.optimizer.core.plan.query.IShowWithoutTable;
import com.taobao.tddl.optimizer.utils.OptimizerToString;

public class ShowWithoutTable extends Show implements IShowWithoutTable {

    @Override
    public String toStringWithInden(int inden) {

        String tabTittle = OptimizerToString.getTab(inden);
        String tabContent = OptimizerToString.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();
        appendln(sb, tabTittle + "SHOW " + this.type);
        appendField(sb, "executeOn", this.getDataNode(), tabContent);

        return sb.toString();
    }

    @Override
    public ShowWithoutTable copy() {
        ShowWithoutTable newShow = new ShowWithoutTable();
        newShow.setType(this.type);
        newShow.setSql(this.sql);
        newShow.executeOn(this.getDataNode());

        return newShow;
    }

}
