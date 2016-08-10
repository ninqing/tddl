package com.taobao.tddl.optimizer.core.ast.dml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.ASTNodeFactory;
import com.taobao.tddl.optimizer.core.ast.DMLNode;
import com.taobao.tddl.optimizer.core.ast.query.KVIndexNode;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.dml.IInsert;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

public class InsertNode extends DMLNode<InsertNode> {

    private boolean           createPk = true;       // 是否为自增长字段，暂时不支持
    private List<Object>      duplicateUpdateValues;
    private List<ISelectable> duplicateUpdateColumns;

    public InsertNode(TableNode table){
        super(table);
    }

    @Override
    public IPut toDataNodeExecutor(int shareIndex) {
        IInsert insert = ASTNodeFactory.getInstance().createInsert();
        if (this.getDuplicateUpdateColumns() != null) {
            List<String> partiionColumns = OptimizerContext.getContext()
                .getRule()
                .getSharedColumns(this.getNode().getTableMeta().getTableName());

            for (ISelectable updateColumn : this.getDuplicateUpdateColumns()) {
                if (partiionColumns.contains(updateColumn.getColumnName())) {
                    throw new IllegalArgumentException("column :" + updateColumn.getColumnName() + " 是分库键，不允许修改");
                }

                if (this.getNode().getTableMeta().getPrimaryKeyMap().containsKey(updateColumn.getColumnName())) {
                    throw new IllegalArgumentException("column :" + updateColumn.getColumnName() + " 是主键，不允许修改");
                }
            }
        }

        if (this.getNode().getActualTableName() != null) {
            insert.setTableName(this.getNode().getActualTableName());
        } else if (this.getNode() instanceof KVIndexNode) {
            insert.setTableName(((KVIndexNode) this.getNode()).getIndexName());
        } else {
            insert.setTableName(this.getNode().getTableName());
        }
        insert.setIndexName((this.getNode()).getIndexUsed().getName());
        insert.setConsistent(true);
        insert.setUpdateColumns(this.getColumns());
        insert.setUpdateValues(this.getValues());

        insert.setIgnore(this.isIgnore());
        insert.setQuick(this.isQuick());
        insert.setLowPriority(this.lowPriority);
        insert.setHighPriority(this.highPriority);
        insert.setDelayed(this.isDelayed());

        insert.setDuplicateUpdateColumns(this.getDuplicateUpdateColumns());
        insert.setDuplicateUpdateValues(this.getDuplicateUpdateValues());
        insert.setMultiValues(this.isMultiValues());
        insert.setMultiValues(this.getMultiValues());

        insert.executeOn(this.getNode().getDataNode());
        insert.setBatchIndexs(this.getBatchIndexs());
        insert.setExistSequenceVal(this.isExistSequenceVal());
        return insert;
    }

    @Override
    public InsertNode deepCopy() {
        InsertNode insert = new InsertNode(null);
        super.deepCopySelfTo(insert);
        insert.setCreatePk(this.isCreatePk());

        insert.setDuplicateUpdateColumns(OptimizerUtils.copySelectables(this.duplicateUpdateColumns));
        insert.setDuplicateUpdateValues(OptimizerUtils.copyValues(this.duplicateUpdateValues));
        return insert;
    }

    @Override
    public InsertNode copy() {
        InsertNode insert = new InsertNode(null);
        super.copySelfTo(insert);
        insert.setCreatePk(this.isCreatePk());
        insert.setDuplicateUpdateColumns(this.getDuplicateUpdateColumns());
        insert.setDuplicateUpdateValues(this.getDuplicateUpdateValues());
        insert.setMultiValues(this.isMultiValues());
        insert.setMultiValues(multiValues);
        return insert;
    }

    public boolean isCreatePk() {
        return createPk;
    }

    public InsertNode setCreatePk(boolean createPk) {
        this.createPk = createPk;
        return this;
    }

    public void duplicateUpdate(String[] updateColumns, Object[] updateValues) {
        List<ISelectable> cs = new LinkedList<ISelectable>();
        for (String name : updateColumns) {
            ISelectable s = OptimizerUtils.createColumnFromString(name);
            cs.add(s);
        }

        List<Object> valueList = new ArrayList<Object>(Arrays.asList(updateValues));

        this.setDuplicateUpdateColumns(cs);
        this.setDuplicateUpdateValues(valueList);

    }

    public void setDuplicateUpdateColumns(List<ISelectable> cs) {
        this.duplicateUpdateColumns = cs;

    }

    public List<Object> getDuplicateUpdateValues() {
        return duplicateUpdateValues;
    }

    public List<ISelectable> getDuplicateUpdateColumns() {
        return duplicateUpdateColumns;
    }

    public void setDuplicateUpdateValues(List<Object> valueList) {
        this.duplicateUpdateValues = valueList;

    }

    @Override
    public void assignment(Parameters parameterSettings) {
        super.assignment(parameterSettings);
        if (duplicateUpdateValues != null) {
            List<Object> comps = new ArrayList<Object>(duplicateUpdateValues.size());
            for (Object comp : duplicateUpdateValues) {
                if (comp instanceof IBindVal) {
                    comps.add(((IBindVal) comp).assignment(parameterSettings));
                } else if (comp instanceof ISelectable) {
                    comps.add(((ISelectable) comp).assignment(parameterSettings));
                } else {
                    comps.add(comp);
                }
            }

            this.setDuplicateUpdateValues(comps);
        }
    }

    @Override
    public void build() {
        super.build();
        if (this.getDuplicateUpdateColumns() != null) {
            for (ISelectable s : this.getDuplicateUpdateColumns()) {
                ISelectable res = null;
                for (Object obj : table.getColumnsReferedForParent()) {
                    ISelectable querySelected = (ISelectable) obj;
                    if (s.isSameName(querySelected)) { // 尝试查找对应的字段信息
                        res = querySelected;
                        break;
                    }
                }

                if (res == null) {
                    throw new IllegalArgumentException("column: " + s.getColumnName() + " is not existed in either "
                                                       + table.getName() + " or select clause");
                }
            }

            convertTypeToSatifyColumnMeta(this.getDuplicateUpdateColumns(), this.getDuplicateUpdateValues());
        }
    }

}
