package com.taobao.tddl.qatest.matrix.basecrud;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.ExecuteTableName;
import com.taobao.tddl.qatest.util.EclipseParameterized;

/**
 * Comment for LocalServerInsertTest
 * <p/>
 * Author By: yaolingling.pt Created Date: 2012-2-20 下午01:40:43
 */
@RunWith(EclipseParameterized.class)
public class BatchInsertTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public BatchInsertTest(String tableName){
        BaseTestCase.normaltblTableName = tableName;
    }

    @Before
    public void initData() throws Exception {
        tddlUpdateData("delete from  " + normaltblTableName, null);
        mysqlUpdateData("delete from  " + normaltblTableName, null);
    }

    @Test
    public void insertAllFieldTest() throws Exception {
        String sql = "insert into " + normaltblTableName + " values(?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList();

        for (int i = 0; i < 100; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(Long.valueOf(RandomStringUtils.randomNumeric(8)));
            param.add(Long.valueOf(RandomStringUtils.randomNumeric(8)));
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            param.add(name);
            param.add(fl);

            params.add(param);
        }
        executeBatch(sql, params);

        sql = "select * from " + normaltblTableName;
        String[] columnParam = { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void insertAllFieldIgnoreTest() throws Exception {

        if (normaltblTableName.startsWith("ob")) {
            // ob不支持
            return;
        }
        String sql = "insert ignore into " + normaltblTableName + " values(?,?,?,?,?,?,?)";

        List<List<Object>> params = new ArrayList();

        for (int i = 0; i < 100; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(1);
            param.add(Long.valueOf(RandomStringUtils.randomNumeric(8)));
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            param.add(name);
            param.add(fl);

            params.add(param);
        }
        executeBatch(sql, params);

        sql = "select * from " + normaltblTableName;
        String[] columnParam = { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void insertAllFieldTestWithFunction() throws Exception {
        String sql = "insert into " + normaltblTableName + " values(?,?,now(),?,?,?,?)";

        List<List<Object>> params = new ArrayList();

        for (int i = 0; i < 100; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(Long.valueOf(RandomStringUtils.randomNumeric(8)));
            param.add(Long.valueOf(RandomStringUtils.randomNumeric(8)));
            param.add(gmt);
            param.add(gmt);
            param.add(name);
            param.add(fl);

            params.add(param);
        }
        executeBatch(sql, params);

        sql = "select * from " + normaltblTableName;
        String[] columnParam = { "PK", "ID", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 有部分列没有使用绑定变量，会造成顺序不一致，测试此时的映射情况
     * 
     * @throws Exception
     */
    @Test
    public void insertAllFieldTestWithSomeFieldCostants() throws Exception {
        String sql = "insert into " + normaltblTableName + " values(?,?,?,?,?,'123',?)";

        List<List<Object>> params = new ArrayList();

        for (int i = 0; i < 100; i++) {
            List<Object> param = new ArrayList<Object>();
            param.add(Long.valueOf(RandomStringUtils.randomNumeric(8)));
            param.add(Long.valueOf(RandomStringUtils.randomNumeric(8)));
            param.add(gmtDay);
            param.add(gmt);
            param.add(gmt);
            // param.add(name);
            param.add(fl);

            params.add(param);
        }
        executeBatch(sql, params);

        sql = "select * from " + normaltblTableName;
        String[] columnParam = { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL", "GMT_TIMESTAMP", "GMT_DATETIME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

}
