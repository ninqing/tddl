package com.taobao.tddl.optimizer.costbased;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.tddl.common.jdbc.ParameterContext;
import com.taobao.tddl.common.jdbc.ParameterMethod;
import com.taobao.tddl.common.jdbc.Parameters;
import com.taobao.tddl.optimizer.BaseOptimizerTest;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;
import com.taobao.tddl.optimizer.core.plan.IPut;
import com.taobao.tddl.optimizer.core.plan.dml.IDelete;
import com.taobao.tddl.optimizer.core.plan.dml.IInsert;
import com.taobao.tddl.optimizer.core.plan.dml.IReplace;
import com.taobao.tddl.optimizer.core.plan.dml.IUpdate;
import com.taobao.tddl.optimizer.core.plan.query.IMerge;
import com.taobao.tddl.optimizer.exceptions.QueryException;

/**
 * 批处理测试
 * 
 * @author jianghang 2014-3-5 下午11:19:48
 * @since 5.0.0
 */
public class BatchTest extends BaseOptimizerTest {

    @Test
    public void testInsert() throws QueryException {
        String sql = "INSERT INTO TABLE1(ID) VALUES(?)";

        Map<Integer, ParameterContext> currentParameter = new HashMap<Integer, ParameterContext>();
        ParameterContext p1 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 2 });
        currentParameter.put(1, p1);

        Parameters parameterSettings = new Parameters();
        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 1 }));
        parameterSettings.addBatch();

        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 2 }));
        parameterSettings.addBatch();

        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(sql, parameterSettings, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(((IInsert) ((IMerge) plan).getSubNodes().get(0)).getBatchIndexs(), Arrays.asList(0));
        Assert.assertEquals(((IInsert) ((IMerge) plan).getSubNodes().get(1)).getBatchIndexs(), Arrays.asList(1));
    }

    @Test
    public void testInsert_多value() throws QueryException {
        String sql = "INSERT INTO TABLE1(ID) VALUES (?),(?),(?)";

        Parameters parameterSettings = new Parameters();
        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 1 }));

        parameterSettings.getCurrentParameter().put(2,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 5 }));

        parameterSettings.getCurrentParameter().put(3,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 9 }));

        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(sql, parameterSettings, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(((IInsert) ((IMerge) plan).getSubNodes().get(0)).getMultiValues(),
            Arrays.asList(Arrays.asList(1), Arrays.asList(9)));
        Assert.assertEquals(((IInsert) ((IMerge) plan).getSubNodes().get(1)).getMultiValues(),
            Arrays.asList(Arrays.asList(5)));
    }

    @Test
    public void testInsert_单库单表() throws QueryException {
        String sql = "INSERT INTO STUDENT(ID) VALUES(?)";

        Map<Integer, ParameterContext> currentParameter = new HashMap<Integer, ParameterContext>();
        ParameterContext p1 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 2 });
        currentParameter.put(1, p1);

        Parameters parameterSettings = new Parameters();
        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 1 }));
        parameterSettings.addBatch();

        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 2 }));
        parameterSettings.addBatch();

        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(sql, parameterSettings, null, false);
        Assert.assertTrue(plan instanceof IInsert);
        Assert.assertEquals(((IInsert) plan).getBatchIndexs(), Arrays.asList(0, 1));
    }

    @Test
    public void testInsert_单库单表_多value() throws QueryException {
        String sql = "INSERT INTO STUDENT(ID) VALUES (?),(?),(?)";

        Parameters parameterSettings = new Parameters();
        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 1 }));

        parameterSettings.getCurrentParameter().put(2,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 5 }));

        parameterSettings.getCurrentParameter().put(3,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 9 }));

        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(sql, parameterSettings, null, false);
        Assert.assertTrue(plan instanceof IInsert);
        Assert.assertTrue(plan.getSql() != null);
    }

    @Test
    public void testPut() throws QueryException {
        String sql = "REPLACE INTO TABLE1(ID) VALUES(?)";

        Map<Integer, ParameterContext> currentParameter = new HashMap<Integer, ParameterContext>();
        ParameterContext p1 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 2 });
        currentParameter.put(1, p1);

        Parameters parameterSettings = new Parameters();
        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 1 }));
        parameterSettings.addBatch();

        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 2 }));
        parameterSettings.addBatch();

        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(sql, parameterSettings, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(((IReplace) ((IMerge) plan).getSubNodes().get(0)).getBatchIndexs(), Arrays.asList(0));
        Assert.assertEquals(((IReplace) ((IMerge) plan).getSubNodes().get(1)).getBatchIndexs(), Arrays.asList(1));
    }

    @Test
    public void testPut_多value() throws QueryException {
        String sql = "REPLACE INTO TABLE1(ID) VALUES (?),(?),(?)";

        Parameters parameterSettings = new Parameters();
        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 1 }));

        parameterSettings.getCurrentParameter().put(2,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 5 }));

        parameterSettings.getCurrentParameter().put(3,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 9 }));

        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(sql, parameterSettings, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(((IPut) ((IMerge) plan).getSubNodes().get(0)).getMultiValues(),
            Arrays.asList(Arrays.asList(1), Arrays.asList(9)));
        Assert.assertEquals(((IPut) ((IMerge) plan).getSubNodes().get(1)).getMultiValues(),
            Arrays.asList(Arrays.asList(5)));
    }

    @Test
    public void testUpdate() throws QueryException {
        String sql = "UPDATE TABLE1 SET NAME = ? WHERE ID = ?";

        Map<Integer, ParameterContext> currentParameter = new HashMap<Integer, ParameterContext>();
        ParameterContext p1 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 2 });
        currentParameter.put(1, p1);

        Parameters parameterSettings = new Parameters();
        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, "hello1" }));
        parameterSettings.getCurrentParameter().put(2,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 2, 1 }));
        parameterSettings.addBatch();

        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, "hello2" }));
        parameterSettings.getCurrentParameter().put(2,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 2, 2 }));
        parameterSettings.addBatch();

        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(sql, parameterSettings, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(((IUpdate) ((IMerge) plan).getSubNodes().get(0)).getBatchIndexs(), Arrays.asList(0));
        Assert.assertEquals(((IUpdate) ((IMerge) plan).getSubNodes().get(1)).getBatchIndexs(), Arrays.asList(1));
    }

    @Test
    public void testPut_单库单表_多value() throws QueryException {
        String sql = "REPLACE INTO STUDENT(ID) VALUES (?),(?),(?)";

        Parameters parameterSettings = new Parameters();
        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 1 }));

        parameterSettings.getCurrentParameter().put(2,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 5 }));

        parameterSettings.getCurrentParameter().put(3,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 9 }));

        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(sql, parameterSettings, null, false);
        Assert.assertTrue(plan instanceof IPut);
        Assert.assertTrue(plan.getSql() != null);
    }

    @Test
    public void testUpdate_范围查询() throws QueryException {
        String sql = "UPDATE TABLE1 SET NAME = ? WHERE ID > ? AND ID < ?";

        Map<Integer, ParameterContext> currentParameter = new HashMap<Integer, ParameterContext>();
        ParameterContext p1 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 2 });
        currentParameter.put(1, p1);

        Parameters parameterSettings = new Parameters();
        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, "hello1" }));
        parameterSettings.getCurrentParameter().put(2,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 2, 1 }));
        parameterSettings.getCurrentParameter().put(3,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 3, 4 }));
        parameterSettings.addBatch();

        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, "hello2" }));
        parameterSettings.getCurrentParameter().put(2,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 2, 2 }));
        parameterSettings.getCurrentParameter().put(3,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 3, 5 }));
        parameterSettings.addBatch();

        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(sql, parameterSettings, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(((IUpdate) ((IMerge) plan).getSubNodes().get(0)).getBatchIndexs(), Arrays.asList(0));
        Assert.assertEquals(((IUpdate) ((IMerge) plan).getSubNodes().get(1)).getBatchIndexs(), Arrays.asList(1));
        Assert.assertEquals(((IUpdate) ((IMerge) plan).getSubNodes().get(2)).getBatchIndexs(), Arrays.asList(0, 1));
    }

    @Test
    public void testUpdate_in查询() throws QueryException {
        String sql = "UPDATE TABLE1 SET NAME = ? WHERE ID IN (?,?)";

        Map<Integer, ParameterContext> currentParameter = new HashMap<Integer, ParameterContext>();
        ParameterContext p1 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 2 });
        currentParameter.put(1, p1);

        Parameters parameterSettings = new Parameters();
        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, "hello1" }));
        parameterSettings.getCurrentParameter().put(2,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 2, 2 }));
        parameterSettings.getCurrentParameter().put(3,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 3, 3 }));
        parameterSettings.addBatch();

        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, "hello2" }));
        parameterSettings.getCurrentParameter().put(2,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 2, 3 }));
        parameterSettings.getCurrentParameter().put(3,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 3, 4 }));
        parameterSettings.addBatch();

        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(sql, parameterSettings, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(((IUpdate) ((IMerge) plan).getSubNodes().get(0)).getBatchIndexs(), Arrays.asList(0));
        Assert.assertEquals(((IUpdate) ((IMerge) plan).getSubNodes().get(1)).getBatchIndexs(), Arrays.asList(1));
        Assert.assertEquals(((IUpdate) ((IMerge) plan).getSubNodes().get(2)).getBatchIndexs(), Arrays.asList(0, 1));
    }

    @Test
    public void testDelete() throws QueryException {
        String sql = "DELETE FROM TABLE1 WHERE ID = ?";

        Map<Integer, ParameterContext> currentParameter = new HashMap<Integer, ParameterContext>();
        ParameterContext p1 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 2 });
        currentParameter.put(1, p1);

        Parameters parameterSettings = new Parameters();
        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 1 }));
        parameterSettings.addBatch();

        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 2 }));
        parameterSettings.addBatch();

        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(sql, parameterSettings, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(((IDelete) ((IMerge) plan).getSubNodes().get(0)).getBatchIndexs(), Arrays.asList(0));
        Assert.assertEquals(((IDelete) ((IMerge) plan).getSubNodes().get(1)).getBatchIndexs(), Arrays.asList(1));
    }

    @Test
    public void testDelete_范围查询() throws QueryException {
        String sql = "DELETE FROM TABLE1 WHERE ID > ? AND ID < ?";

        Map<Integer, ParameterContext> currentParameter = new HashMap<Integer, ParameterContext>();
        ParameterContext p1 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 2 });
        currentParameter.put(1, p1);

        Parameters parameterSettings = new Parameters();
        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 1 }));
        parameterSettings.getCurrentParameter().put(2,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 2, 4 }));
        parameterSettings.addBatch();

        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 2 }));
        parameterSettings.getCurrentParameter().put(2,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 2, 5 }));
        parameterSettings.addBatch();

        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(sql, parameterSettings, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(((IDelete) ((IMerge) plan).getSubNodes().get(0)).getBatchIndexs(), Arrays.asList(0));
        Assert.assertEquals(((IDelete) ((IMerge) plan).getSubNodes().get(1)).getBatchIndexs(), Arrays.asList(1));
        Assert.assertEquals(((IDelete) ((IMerge) plan).getSubNodes().get(2)).getBatchIndexs(), Arrays.asList(0, 1));
    }

    @Test
    public void testDelete_in查询() throws QueryException {
        String sql = "DELETE FROM TABLE1 WHERE ID IN (?,?)";

        Map<Integer, ParameterContext> currentParameter = new HashMap<Integer, ParameterContext>();
        ParameterContext p1 = new ParameterContext(ParameterMethod.setObject1, new Object[] { 2 });
        currentParameter.put(1, p1);

        Parameters parameterSettings = new Parameters();
        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 2 }));
        parameterSettings.getCurrentParameter().put(2,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 2, 3 }));
        parameterSettings.addBatch();

        parameterSettings.getCurrentParameter().put(1,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 1, 3 }));
        parameterSettings.getCurrentParameter().put(2,
            new ParameterContext(ParameterMethod.setObject1, new Object[] { 2, 4 }));
        parameterSettings.addBatch();

        IDataNodeExecutor plan = optimizer.optimizeAndAssignment(sql, parameterSettings, null, false);
        Assert.assertTrue(plan instanceof IMerge);
        Assert.assertEquals(((IDelete) ((IMerge) plan).getSubNodes().get(0)).getBatchIndexs(), Arrays.asList(0));
        Assert.assertEquals(((IDelete) ((IMerge) plan).getSubNodes().get(1)).getBatchIndexs(), Arrays.asList(1));
        Assert.assertEquals(((IDelete) ((IMerge) plan).getSubNodes().get(2)).getBatchIndexs(), Arrays.asList(0, 1));
    }
}
