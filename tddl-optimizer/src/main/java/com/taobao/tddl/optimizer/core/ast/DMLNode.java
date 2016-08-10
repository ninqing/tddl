package com.taobao.tddl.optimizer.core.ast;

import java.util.ArrayList;
import java.util.List;

import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.config.table.TableMeta;
import com.taobao.tddl.optimizer.core.ast.query.TableNode;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.expression.IBindVal;
import com.taobao.tddl.optimizer.core.expression.IFunction;
import com.taobao.tddl.optimizer.core.expression.ISelectable;
import com.taobao.tddl.optimizer.core.expression.ISequenceVal;
import com.taobao.tddl.optimizer.core.expression.bean.NullValue;
import com.taobao.tddl.optimizer.costbased.SubQueryPreProcessor;
import com.taobao.tddl.optimizer.utils.OptimizerToString;
import com.taobao.tddl.optimizer.utils.OptimizerUtils;

import com.taobao.tddl.common.utils.logger.Logger;
import com.taobao.tddl.common.utils.logger.LoggerFactory;

/**
 * DML操作树
 * 
 * @since 5.0.0
 */
public abstract class DMLNode<RT extends DMLNode> extends ASTNode<RT> {

    protected static final Logger logger            = LoggerFactory.getLogger(DMLNode.class);
    protected List<ISelectable>   columns;
    protected List<Object>        values;
    protected boolean             isMultiValues     = false;
    protected List<List<Object>>  multiValues;
    // 直接依赖为tableNode，如果涉及多库操作，会是一个Merge下面挂多个DML
    protected TableNode           table             = null;
    protected Parameters          parameterSettings = null;
    protected boolean             needBuild         = true;
    protected List<Integer>       batchIndexs       = new ArrayList<Integer>();
    protected boolean             ignore            = false;
    protected boolean             lowPriority       = false;
    protected boolean             highPriority      = false;
    protected boolean             delayed           = false;
    protected boolean             quick             = false;

    public DMLNode(){
        super();
    }

    public boolean isIgnore() {
        return ignore;
    }

    public DMLNode setIgnore(boolean ignore) {
        this.ignore = ignore;
        return this;
    }

    public boolean isLowPriority() {
        return lowPriority;
    }

    public DMLNode setLowPriority(boolean lowPriority) {
        this.lowPriority = lowPriority;
        return this;
    }

    public boolean isHighPriority() {
        return highPriority;
    }

    public DMLNode setHighPriority(boolean highPriority) {
        this.highPriority = highPriority;
        return this;
    }

    public boolean isDelayed() {
        return delayed;
    }

    public DMLNode setDelayed(boolean delayed) {
        this.delayed = delayed;

        return this;
    }

    public boolean isQuick() {
        return quick;
    }

    public DMLNode setQuick(boolean quick) {
        this.quick = quick;

        return this;
    }

    public DMLNode(TableNode table){
        this.table = table;
    }

    public DMLNode setParameterSettings(Parameters parameterSettings) {
        this.parameterSettings = parameterSettings;
        return this;
    }

    public TableNode getNode() {
        return this.table;
    }

    public DMLNode setNode(TableNode table) {
        this.table = table;
        return this;
    }

    public DMLNode setColumns(List<ISelectable> columns) {
        this.columns = columns;
        return this;
    }

    public List<ISelectable> getColumns() {
        return this.columns;
    }

    public DMLNode setValues(List<Object> values) {
        this.values = values;
        return this;
    }

    public List<Object> getValues() {
        return this.values;
    }

    public TableMeta getTableMeta() {
        return getNode().getTableMeta();
    }

    @Override
    public boolean isNeedBuild() {
        return needBuild;
    }

    protected void setNeedBuild(boolean needBuild) {
        this.needBuild = needBuild;
    }

    public boolean isMultiValues() {
        return isMultiValues;
    }

    public DMLNode setMultiValues(boolean isMutiValues) {
        this.isMultiValues = isMutiValues;
        return this;
    }

    public List<List<Object>> getMultiValues() {
        return multiValues;
    }

    public DMLNode setMultiValues(List<List<Object>> multiValues) {
        this.multiValues = multiValues;
        return this;
    }

    public List<Object> getValues(int index) {
        if (this.isMultiValues) {
            if (this.multiValues != null) {
                return this.multiValues.get(index);
            } else {
                return null;
            }
        }

        return this.values;
    }

    public int getMultiValuesSize() {
        if (this.isMultiValues) {
            return this.multiValues.size();
        } else {
            return 1;
        }
    }

    @Override
    public void build() {
        if (this.table != null) {
            table.build();
        }

        if ((this.getColumns() == null || this.getColumns().isEmpty())
            && (this.getValues(0) == null || this.getValues(0).isEmpty())) {
            return;
        }

        if (columns == null || columns.isEmpty()) { // 如果字段为空，默认为所有的字段数据的,比如insert所有字段
            columns = OptimizerUtils.columnMetaListToIColumnList(this.getTableMeta().getAllColumns(),
                this.getTableMeta().getTableName());
        }

        boolean isMatch = true;
        if (isMultiValues) {
            for (List<Object> vs : multiValues) {
                for (Object v : vs) {
                    if (v instanceof ISequenceVal) {
                        existSequenceVal = true;
                    }
                }
                isMatch &= (vs.size() == columns.size());
            }
        } else {
            isMatch &= (values.size() == columns.size());
            for (Object v : values) {
                if (v instanceof ISequenceVal) {
                    existSequenceVal = true;
                }
            }
        }

        if (!isMatch) {
            throw new IllegalArgumentException("The size of the columns and values is not matched.");
        }

        for (ISelectable s : this.getColumns()) {
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

        convertTypeToSatifyColumnMeta(this.getColumns(), this.getValues(0));
    }

    /**
     * 尝试根据字段类型进行value转化
     */
    protected List<Object> convertTypeToSatifyColumnMeta(List<ISelectable> cs, List<Object> vs) {
        for (int i = 0; i < cs.size(); i++) {
            Comparable c = cs.get(i);
            Object v = vs.get(i);
            DataType type = null;
            if (v == null || v instanceof IBindVal || v instanceof ISequenceVal || v instanceof NullValue) {
                continue;
            }

            if (c instanceof ISelectable) {
                type = ((ISelectable) c).getDataType();
            }

            vs.set(i, OptimizerUtils.convertType(v, type));
        }
        return vs;
    }

    @Override
    public RT executeOn(String dataNode) {
        super.executeOn(dataNode);
        return (RT) this;
    }

    @Override
    public void assignment(Parameters parameterSettings) {
        QueryTreeNode qct = getNode();

        if (qct != null) {
            qct.assignment(parameterSettings);
        }

        if (values != null) {
            List<Object> comps = new ArrayList<Object>(values.size());
            for (Object comp : values) {
                if (comp instanceof IBindVal) {
                    comps.add(((IBindVal) comp).assignment(parameterSettings));
                } else if (comp instanceof ISelectable) {
                    comps.add(((ISelectable) comp).assignment(parameterSettings));
                } else {
                    comps.add(comp);
                }
            }

            this.setValues(comps);
        }

        if (multiValues != null) {
            List<List<Object>> multiValues = new ArrayList<List<Object>>(this.multiValues.size());
            for (List<Object> values : this.multiValues) {
                List<Object> comps = new ArrayList<Object>();
                for (Object comp : values) {
                    if (comp instanceof IBindVal) {
                        comps.add(((IBindVal) comp).assignment(parameterSettings));
                    } else if (comp instanceof ISelectable) {
                        comps.add(((ISelectable) comp).assignment(parameterSettings));
                    } else {
                        comps.add(comp);
                    }
                }

                multiValues.add(comps);
            }

            this.setMultiValues(multiValues);
        }
    }

    public IFunction getNextSubqueryOnFilter() {
        IFunction func = SubQueryPreProcessor.findNextSubqueryOnFilter(this.getNode());
        if (func != null) {
            return (IFunction) func.copy();
        } else {
            return null;
        }
    }

    /**
     * 这个节点上执行哪些batch
     * 
     * @return
     */
    public List<Integer> getBatchIndexs() {
        return batchIndexs;
    }

    public void setBatchIndexs(List<Integer> batchIndexs) {
        this.batchIndexs = batchIndexs;
    }

    protected void copySelfTo(DMLNode to) {
        to.columns = this.columns;
        to.values = this.values;
        to.table = this.table;
        to.multiValues = this.multiValues;
        to.isMultiValues = this.isMultiValues;
        to.existSequenceVal = this.existSequenceVal;

        to.lowPriority = this.lowPriority;
        to.highPriority = this.highPriority;
        to.delayed = this.delayed;
        to.ignore = this.ignore;
        to.quick = this.quick;
        to.batchIndexs = this.batchIndexs;
    }

    protected void deepCopySelfTo(DMLNode to) {
        to.columns = OptimizerUtils.copySelectables(this.columns);
        if (this.values != null) {
            to.values = new ArrayList(this.values.size());

            for (Object value : this.values) {
                if (value instanceof ISelectable) {
                    to.values.add(((ISelectable) value).copy());
                } else if (value instanceof IBindVal) {
                    to.values.add(((IBindVal) value).copy());
                } else {
                    to.values.add(value);
                }
            }
        }

        to.setMultiValues(this.isMultiValues());
        if (this.multiValues != null) {
            List<List<Object>> multiValues = new ArrayList<List<Object>>(this.multiValues.size());
            for (List<Object> value : this.multiValues) {
                multiValues.add(OptimizerUtils.copyValues(value));
            }

            to.setMultiValues(multiValues);
        }

        to.table = this.table.deepCopy();
        to.existSequenceVal = this.existSequenceVal;

        to.lowPriority = this.lowPriority;
        to.highPriority = this.highPriority;
        to.delayed = this.delayed;
        to.ignore = this.ignore;
        to.quick = this.quick;
        to.batchIndexs = new ArrayList<Integer>(this.batchIndexs);
    }

    @Override
    public RT copySelf() {
        return copy();
    }

    @Override
    public String toString(int inden, int shareIndex) {
        String tabTittle = OptimizerToString.getTab(inden);
        String tabContent = OptimizerToString.getTab(inden + 1);
        StringBuilder sb = new StringBuilder();
        OptimizerToString.appendln(sb, tabTittle + this.getClass().getSimpleName());

        OptimizerToString.appendField(sb, "columns", this.getColumns(), tabContent);
        if (isMultiValues()) {
            OptimizerToString.appendField(sb, "multiValues", this.getMultiValues(), tabContent);
        } else {
            OptimizerToString.appendField(sb, "values", this.getValues(), tabContent);
        }

        if (this.getNode() != null) {
            OptimizerToString.appendln(sb, tabContent + "query:");
            sb.append(this.getNode().toString(inden + 2));
        }
        return sb.toString();
    }

}
