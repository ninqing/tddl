package com.taobao.tddl.qatest.matrix.select;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.ExecuteTableName;
import com.taobao.tddl.qatest.util.EclipseParameterized;

/**
 * Comment for LocalServerSelectTest
 * <p/>
 * Author By: yaolingling.pt Created Date: 2012-2-17 11:30:55
 */
@RunWith(EclipseParameterized.class)
public class SelectTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.studentTable(dbType));
    }

    public SelectTest(String studentTableName){
        BaseTestCase.studentTableName = studentTableName;
    }

    @Before
    public void initData() throws Exception {
        tddlUpdateData("delete from  " + studentTableName, null);
        mysqlUpdateData("delete from  " + studentTableName, null);
    }

    @Test
    public void selectAllFieldTest() throws Exception {
        tddlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        mysqlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        String sql = "select * from " + studentTableName + " where id=" + RANDOM_ID;
        String[] columnParam = { "NAME", "SCHOOL" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void selectAllFieldWithFuncTest() throws Exception {
        tddlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        mysqlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        String sql = "select *,count(*) from " + studentTableName + " where id=" + RANDOM_ID;
        String[] columnParam = { "NAME", "SCHOOL", "id", "count(*)" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void selectSomeFieldTest() throws Exception {
        tddlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        mysqlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        String sql = "select id,name from " + studentTableName + " where id=" + RANDOM_ID;
        String[] columnParam = { "NAME", "ID" };
        selectOrderAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select id,name from " + studentTableName + " where name= ?";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        selectOrderAssert(sql, columnParam, param);
    }

    @Test
    public void selectWithQuotationTest() throws Exception {
        String name = "as'sdfd's";
        tddlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        mysqlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        String[] columnParam = { "NAME", "ID" };

        String sql = "select id,name from " + studentTableName + " where name= ?";
        List<Object> param = new ArrayList<Object>();
        param.add(name);
        selectOrderAssert(sql, columnParam, param);

        sql = "select id,name from " + studentTableName + " where name= 'as\\'sdfd\\'s'";
        selectOrderAssert(sql, columnParam, null);
    }

    @Test
    public void selectWithNotExistDateTest() throws Exception {
        tddlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        mysqlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        String sql = "select * from " + studentTableName + " where id=" + RANDOM_ID + 1;

        selectConutAssert(sql, Collections.EMPTY_LIST);
    }

    @Test
    public void selectWithNotExistFileTest() throws Exception {
        tddlUpdateData("insert into " + studentTableName + " (id,name,school) values (?,?,?)",
            Arrays.asList(new Object[] { RANDOM_ID, name, school }));
        String sql = "select * from " + studentTableName + " where pk=" + RANDOM_ID;
        try {
            rc = tddlQueryData(sql, null);
            rc.next();
            Assert.fail();
        } catch (Exception ex) {
            // Assert.assertTrue(ex.getMessage().contains("column: PK is not existed in"));
        }
    }

    @Test
    public void selectWithNotExistTableTest() throws Exception {
        String sql = "select * from stu where pk=" + RANDOM_ID;
        try {
            rc = tddlQueryData(sql, null);
            rc.next();
            Assert.fail();
        } catch (Exception ex) {
            // Assert.assertTrue(ex.getMessage().contains("STU is not found"));
        }
    }
}
