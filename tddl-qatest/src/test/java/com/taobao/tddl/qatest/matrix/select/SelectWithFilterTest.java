package com.taobao.tddl.qatest.matrix.select;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.ExecuteTableName;
import com.taobao.tddl.qatest.util.EclipseParameterized;

/**
 * 带条件的选择查询
 * <p/>
 * Author By: yaolingling.pt Created Date: 2012-2-21 上午11:30:18
 */
@RunWith(EclipseParameterized.class)
public class SelectWithFilterTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table0={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public SelectWithFilterTest(String normaltblTableName){
        BaseTestCase.normaltblTableName = normaltblTableName;
    }

    @Before
    public void prepareDate() throws Exception {
        normaltblPrepare(0, 20);
    }

    @Test
    public void greaterTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk)>? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(5L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    @Test
    public void greaterEqualTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk)>=? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    @Test
    public void lessTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk)<? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(5L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    @Test
    public void lessEqualTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk)<=? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(9L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    @Test
    public void equalTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk)=? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    @Test
    public void inTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk) in (?) order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    @Test
    public void notEqualTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk) != ? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    @Test
    public void andTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk) != ? and count(pk) < ? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        param.add(11L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    @Test
    public void orTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having count(pk) != ? or count(pk) < ? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        param.add(11L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    @Test
    public void andOrTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having (count(pk) != ? or count(pk) < ?) and count(pk)>? order by name ";
        List<Object> param = new ArrayList<Object>();
        param.add(10L);
        param.add(11L);
        param.add(5L);
        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    @Test
    public void constantTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName + " group by name having 1 order by name ";
        List<Object> param = new ArrayList<Object>();

        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    @Test
    public void constantTest2() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName + " group by name having true order by name ";
        List<Object> param = new ArrayList<Object>();

        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    @Test
    public void constantTest3() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName + " group by name having 'true' order by name ";
        List<Object> param = new ArrayList<Object>();

        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    @Test
    public void constantAndTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName
                     + " group by name having 1 and 2 order by name ";
        List<Object> param = new ArrayList<Object>();

        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

    @Test
    public void constantOrTest() throws Exception {
        String sql = "select name,count(pk) from " + normaltblTableName + " group by name having 1 or 2 order by name ";
        List<Object> param = new ArrayList<Object>();

        String[] columnParam = { "NAME", "COUNT(pk)" };
        selectOrderAssert(sql, columnParam, param);
    }

}
