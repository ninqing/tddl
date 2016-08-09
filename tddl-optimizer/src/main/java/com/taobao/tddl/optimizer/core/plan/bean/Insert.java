package com.taobao.tddl.optimizer.core.plan.bean;

import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendField;
import static com.taobao.tddl.optimizer.utils.OptimizerToString.appendln;

import java.util.List;

import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.dml.IInsert;
import com.taobao.tddl.optimizer.utils.OptimizerToString;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

public class Insert extends Put<IInsert> implements IInsert {

    private List<ISelectable> duplicateUpdateColumns;
    private List<Object>      duplicateUpdateValues;

    public Insert(){
        putType = PUT_TYPE.INSERT;
    }

    @Override
    public void setDuplicateUpdateColumns(List<ISelectable> cs) {
        this.duplicateUpdateColumns = cs;
    }

    @Override
    public List<Object> getDuplicateUpdateValues() {
        return duplicateUpdateValues;
    }

    @Override
    public List<ISelectable> getDuplicateUpdateColumns() {
        return duplicateUpdateColumns;
    }

    @Override
    public void setDuplicateUpdateValues(List<Object> valueList) {
        this.duplicateUpdateValues = valueList;
    }

    @Override
    public IInsert copy() {
        IInsert insert = ASTNodeFactory.getInstance().createInsert();
        copySelfTo(insert);
        insert.setDuplicateUpdateColumns(OptimizerUtils.copySelectables(duplicateUpdateColumns));
        insert.setDuplicateUpdateValues(OptimizerUtils.copyValues(duplicateUpdateValues));
        return insert;
    }

    @Override
    public String toStringWithInden(int inden) {
        String tabTittle = OptimizerToString.getTab(inden);
        String tabContent = OptimizerToString.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();
        appendln(sb, tabTittle + "Put:" + this.getPutType());
        appendField(sb, "tableName", this.getTableName(), tabContent);
        appendField(sb, "indexName", this.getIndexName(), tabContent);
        appendField(sb, "columns", this.getUpdateColumns(), tabContent);
        if (this.isMultiValues()) {
            appendField(sb, "multiValues", this.getMultiValues(), tabContent);
        } else {
            appendField(sb, "values", this.getUpdateValues(), tabContent);
        }

        appendField(sb, "duplicateColumns", this.getDuplicateUpdateColumns(), tabContent);
        appendField(sb, "duplicateValues", this.getDuplicateUpdateValues(), tabContent);

        appendField(sb, "requestId", this.getRequestId(), tabContent);
        appendField(sb, "subRequestId", this.getSubRequestId(), tabContent);
        appendField(sb, "thread", this.getThread(), tabContent);
        appendField(sb, "hostname", this.getRequestHostName(), tabContent);
        appendField(sb, "batchIndexs", this.getBatchIndexs(), tabContent);
        if (this.getQueryTree() != null) {
            appendln(sb, tabContent + "query:");
            sb.append(this.getQueryTree().toStringWithInden(inden + 2));
        }

        return sb.toString();
    }
}
