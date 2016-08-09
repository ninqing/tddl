package com.taobao.tddl.optimizer.parse.cobar.visitor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.cobar.parser.ast.expression.primary.Identifier;
import com.alibaba.cobar.parser.ast.fragment.tableref.TableRefFactor;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowColumns;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowCreate;
import com.alibaba.cobar.parser.ast.stmt.dal.ShowCreate.Type;
import com.alibaba.cobar.parser.ast.stmt.ddl.DescTableStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLDeleteStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLInsertStatement;
import com.alibaba.cobar.parser.ast.stmt.dml.DMLReplaceStatement;
import com.alibaba.cobar.parser.visitor.EmptySQLASTVisitor;

/**
 * 提取sql中的表名
 * 
 * @author <a href="junyu@taobao.com">junyu</a>
 * @date 2012-10-26上午09:56:35
 */
public class MysqlTableVisitor extends EmptySQLASTVisitor {

    // schema名对应表名
    private Map<String, String> tablesWithSchema = new HashMap();
    private Set<String>         tables           = new HashSet();

    @Override
    public void visit(TableRefFactor node) {
        if (node.getTable().getParent() != null) {
            tablesWithSchema.put(node.getTable().getParent().getIdTextUpUnescape(), node.getTable()
                .getIdTextUpUnescape());
        } else {
            tables.add(node.getTable().getIdTextUpUnescape());
        }
    }

    @Override
    public void visit(ShowCreate node) {
        if (((ShowCreate) node).getType() == Type.TABLE) {
            if (((ShowCreate) node).getId().getParent() != null) {
                tablesWithSchema.put(((ShowCreate) node).getId().getParent().getIdTextUpUnescape(),
                    ((ShowCreate) node).getId().getIdTextUpUnescape());
            } else {
                tables.add(((ShowCreate) node).getId().getIdTextUpUnescape());
            }

        }
    }

    @Override
    public void visit(DescTableStatement node) {
        if (node.getTable().getParent() != null) {
            tablesWithSchema.put(node.getTable().getParent().getIdTextUpUnescape(), node.getTable()
                .getIdTextUpUnescape());
        } else {
            tables.add(node.getTable().getIdTextUpUnescape());
        }
    }

    @Override
    public void visit(ShowColumns node) {
        if (((ShowColumns) node).getTable().getParent() != null) {
            tablesWithSchema.put(((ShowColumns) node).getTable().getParent().getIdTextUpUnescape(),
                ((ShowColumns) node).getTable().getIdTextUpUnescape());
        } else {
            tables.add(((ShowColumns) node).getTable().getIdTextUpUnescape());
        }

    }

    public Set<String> getTablesWithoutSchema() {
        return tables;
    }

    public Map<String, String> getTablesWithSchema() {
        return tablesWithSchema;
    }

    @Override
    public void visit(DMLDeleteStatement node) {
        List<Identifier> tableNames = node.getTableNames();
        for (Identifier id : tableNames) {
            if (id.getIdTextUpUnescape().equals("*")) {
                tables.add(id.getParent().getIdTextUpUnescape());
            } else {
                tables.add(id.getIdTextUpUnescape());
            }
        }

        super.visit(node);
    }

    @Override
    public void visit(DMLInsertStatement node) {
        Identifier tb = node.getTable();
        tables.add(tb.getIdTextUpUnescape());
        super.visit(node);
    }

    @Override
    public void visit(DMLReplaceStatement node) {
        Identifier tb = node.getTable();
        tables.add(tb.getIdTextUpUnescape());
        super.visit(node);
    }
}
