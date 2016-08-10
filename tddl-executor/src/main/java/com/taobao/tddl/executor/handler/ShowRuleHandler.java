package com.taobao.tddl.executor.handler;

import java.util.List;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.executor.cursor.impl.ArrayResultCursor;
import com.taobao.tddl.optimizer.OptimizerContext;
import com.taobao.tddl.optimizer.core.datatype.DataType;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.rule.OptimizerRule;
import com.taobao.tddl.rule.TableRule;

/**
 * 返回一个表的拓扑信息
 * 
 * @author mengshi.sunmengshi 2014年5月9日 下午5:27:06
 * @since 5.1.0
 */
public class ShowRuleHandler extends HandlerCommon {

    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {

        ArrayResultCursor result = new ArrayResultCursor("BROADCASTS", executionContext);
        result.addColumn("ID", DataType.IntegerType);
        result.addColumn("TABLE_NAME", DataType.StringType);
        result.addColumn("BROADCAST", DataType.BooleanType);
        result.addColumn("ALLOW_FULL_TABLE_SCAN", DataType.BooleanType);
        result.addColumn("DB_NAME_PATTERN", DataType.StringType);
        result.addColumn("DB_RULES_STR", DataType.StringType);
        result.addColumn("TB_NAME_PATTERN", DataType.StringType);
        result.addColumn("TB_RULES_STR", DataType.StringType);
        result.addColumn("PARTITION_KEYS", DataType.StringType);
        result.addColumn("DEFAULT_DB_INDEX", DataType.StringType);
        result.initMeta();
        int index = 0;
        OptimizerRule rule = OptimizerContext.getContext().getRule();
        List<TableRule> tables = rule.getTableRules();
        for (TableRule table : tables) {

            result.addRow(new Object[] { index++, table.getVirtualTbName(), table.isBroadcast(),
                    table.isAllowFullTableScan(), table.getDbNamePattern(), table.getDbRuleStrs(),
                    table.getTbNamePattern(), buildStr(table.getTbRulesStrs()), table.getShardColumns(),
                    rule.getDefaultDbIndex(table.getVirtualTbName()) });

        }

        return result;
    }

    public String buildStr(Object[] list) {
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        for (Object o : list) {
            if (!first) {
                sb.append("\n");
            }
            sb.append(String.valueOf(o));
            first = false;

        }

        return sb.toString();
    }
}
