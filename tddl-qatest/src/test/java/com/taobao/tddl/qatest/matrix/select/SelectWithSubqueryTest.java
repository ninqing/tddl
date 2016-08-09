package com.taobao.tddl.qatest.matrix.select;

import java.util.Arrays;
import java.util.Collections;
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
 * 子查询测试 >,>=,<,<=,=,!=,like all any
 * 
 * @author mengshi.sunmengshi 2014年4月8日 下午4:50:54
 * @since 5.1.0
 */
@RunWith(EclipseParameterized.class)
public class SelectWithSubqueryTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.normaltblStudentTable(dbType));
    }

    public SelectWithSubqueryTest(String normaltblTableName, String studentTableName){
        BaseTestCase.normaltblTableName = normaltblTableName;
        BaseTestCase.studentTableName = studentTableName;
    }

    @Before
    public void prepareDate() throws Exception {
        normaltblPrepare(0, 20);
        studentPrepare(0, MAX_DATA_SIZE);
    }

    @Test
    public void inTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk in (select id from " + studentTableName + ")";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void equalTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk = (select id from " + studentTableName
                     + " order by id limit 1)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void equalMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk = (select max(id) from " + studentTableName
                     + ")";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void greaterTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk > (select id from " + studentTableName
                     + " order by id asc limit 5,1)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void greaterEqualTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk >= (select id from " + studentTableName
                     + " order by id asc limit 5,1)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void lessTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk < (select id from " + studentTableName
                     + " order by id asc limit 5,1)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void lessEqualTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk <= (select id from " + studentTableName
                     + " order by id asc limit 5,1)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void notEqualTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk != (select id from " + studentTableName
                     + " order by id asc limit 5,1)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void likeTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where name like (select name from " + studentTableName
                     + " order by id asc limit 5,1)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void greaterAnyTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk > any(select id from " + studentTableName + ")";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void greaterEqualAnyTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk >= any(select id from " + studentTableName
                     + ")";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void lessAnyTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk < any(select id from " + studentTableName + ")";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void lessEqualAnyTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk <= any(select id from " + studentTableName
                     + ")";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void notEqualAnyTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk != any(select id from " + studentTableName
                     + ")";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void notEqualAnyOneValueTest() throws Exception {
        String sql = "select * from " + normaltblTableName + " where pk != any(select id from " + studentTableName
                     + " where id=1)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void equalAnyTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk = any(select id from " + studentTableName + ")";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void equalAnyMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk = any(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void greatAnyMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk > any(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void greatEqualAnyMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk >= any(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void lessAnyMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk < any(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void lessEqualAnyMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk <= any(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 多个值的
     * 
     * @throws Exception
     */
    @Test
    public void notEqualAnyMaxSomeValueTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk != any(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    /**
     * 一个值的
     * 
     * @throws Exception
     */
    @Test
    public void notEqualAnyMaxOneValueTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk != any(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void equalAllMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk = all(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void equalAllMaxOneValueTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk = all(select max(id) from " + studentTableName
                     + " )";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void greatAllMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk > all(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void greatEqualAllMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk >= all(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void lessAllMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk < all(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void lessEqualAllMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk <= all(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void notEqualAllMaxTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk != all(select max(id) from " + studentTableName
                     + " group by id)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void equalAllOneValueTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk = all(select id from " + studentTableName
                     + " where id=1)";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void equalAllSomeValueTest() throws Exception {

        String sql = "select * from " + normaltblTableName + " where pk = all(select id from " + studentTableName + ")";
        String[] columnParam = { "PK", "NAME" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }
}
