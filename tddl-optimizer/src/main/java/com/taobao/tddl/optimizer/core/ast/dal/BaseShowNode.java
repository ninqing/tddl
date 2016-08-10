package com.taobao.tddl.optimizer.core.ast.dal;

import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.expression.IFilter;

public abstract class BaseShowNode extends ASTNode {

    public enum ShowType {
        SEQUENCES, PARTITIONS, TABLES, TOPOLOGY, BRAODCASTS, RULE
    }

    protected ShowType type;
    protected IFilter  whereFilter;

    public ShowType getType() {
        return type;
    }

    public void setType(ShowType type) {
        this.type = type;
    }

    public IFilter getWhereFilter() {
        return whereFilter;
    }

    public void setWhereFilter(IFilter whereFilter) {
        this.whereFilter = whereFilter;
    }

}
