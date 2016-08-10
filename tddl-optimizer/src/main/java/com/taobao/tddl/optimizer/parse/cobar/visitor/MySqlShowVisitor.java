package com.taobao.tddl.optimizer.parse.cobar.visitor;

import com.alibaba.cobar.parser.ast.stmt.dal.ShowBroadcasts;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowPartitions;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowRule;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowTables;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowTopology;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;
import com.taobao.tddl.optimizer.core.ast.ASTNode;
import com.taobao.tddl.optimizer.core.ast.dal.BaseShowNode.ShowType;
import com.taobao.tddl.optimizer.core.ast.dal.ShowPartitionsNode;
import com.taobao.tddl.optimizer.core.ast.dal.ShowTablesNode;
import com.taobao.tddl.optimizer.core.ast.dal.ShowWithTableNode;
import com.taobao.tddl.optimizer.core.ast.dal.ShowWithoutTableNode;

public class MySqlShowVisitor extends EmptySQLASTVisitor {

    private ASTNode node;

    @Override
    public void visit(ShowTopology showTopology) {
        String name = showTopology.getName().getIdTextUpUnescape();
        ShowWithTableNode node = new ShowWithTableNode(name);
        node.setType(ShowType.TOPOLOGY);

        this.node = node;
    }

    @Override
    public void visit(ShowPartitions showPartitions) {
        String name = showPartitions.getName().getIdTextUpUnescape();
        node = new ShowPartitionsNode(name);
    }

    @Override
    public void visit(ShowTables showTables) {
        node = new ShowTablesNode(showTables.isFull(), null, showTables.getPattern(), null);
    }

    @Override
    public void visit(ShowBroadcasts showBroadcasts) {
        ShowWithoutTableNode node = new ShowWithoutTableNode();
        node.setType(ShowType.BRAODCASTS);
        this.node = node;
    }

    @Override
    public void visit(ShowRule showRule) {
        ShowWithoutTableNode node = new ShowWithoutTableNode();
        node.setType(ShowType.RULE);

        if (showRule.getWhere() != null) {

            MySqlExprVisitor visitor = new MySqlExprVisitor();
            showRule.getWhere().accept(visitor);

            node.setWhereFilter(visitor.getFilter());
        }
        this.node = node;
    }

    public ASTNode getNode() {
        return this.node;
    }
}
