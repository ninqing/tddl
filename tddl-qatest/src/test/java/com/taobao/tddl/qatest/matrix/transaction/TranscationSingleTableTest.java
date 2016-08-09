package com.taobao.tddl.qatest.matrix.transaction;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;
import com.taobao.tddl.qatest.ExecuteTableName;
import com.taobao.tddl.qatest.util.EclipseParameterized;

@RunWith(EclipseParameterized.class)
public class TranscationSingleTableTest extends BaseMatrixTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableName.normaltblTable(dbType));
    }

    public TranscationSingleTableTest(String normaltblTableName){
        BaseTestCase.normaltblTableName = normaltblTableName;
    }

    @Before
    public void initData() throws Exception {

        tddlUpdateData("DELETE FROM " + normaltblTableName, null);
        mysqlUpdateData("DELETE FROM " + normaltblTableName, null);
    }

    @After
    public void destory() throws Exception {
        psConRcRsClose(rc, rs);

        tddlConnection.setAutoCommit(true);

        mysqlConnection.setAutoCommit(true);
    }

    @Test
    public void InsertCommitTest() throws Exception {
        String sql = "insert into " + normaltblTableName + " values(?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(gmt);
        param.add(name);
        param.add(fl);

        tddlConnection.setAutoCommit(false);
        String[] columnParam = { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL" };

        mysqlConnection.setAutoCommit(false);
        try {
            int mysqlAffectRow = mysqlUpdateDataTranscation(sql, param);
            int andorAffectRow = tddlUpdateDataTranscation(sql, param);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            // 在事物内内查到数据
            sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;

            selectOrderAssertTranscation(sql, columnParam, null);

            mysqlConnection.commit();
            tddlConnection.commit();
        } catch (Exception ex) {
            try {
                mysqlConnection.rollback();
                tddlConnection.rollback();
            } catch (Exception ee) {

            }
        }
        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        // 在事物提交正确查询数据
        selectOrderAssertTranscation(sql, columnParam, null);

        // 多次回滚和提交保证不出现异常
        try {
            tddlConnection.commit();
            tddlConnection.commit();
            tddlConnection.rollback();
            tddlConnection.rollback();
        } catch (Exception e) {
            Assert.fail(e.toString());
        }
    }

    @Test
    public void insertSeveralCommitTest() throws Exception {
        String sql = "insert into " + normaltblTableName + " values(?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(gmt);
        param.add(name);
        param.add(fl);

        tddlConnection.setAutoCommit(false);
        String[] columnParam = { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL" };

        mysqlConnection.setAutoCommit(false);
        try {
            int mysqlAffectRow = mysqlUpdateDataTranscation(sql, param);
            int andorAffectRow = tddlUpdateDataTranscation(sql, param);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            // 在事物内内查到数据
            sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;

            selectOrderAssertTranscation(sql, columnParam, null);

            // 未提交，使用不同的连接失败
            Connection otherAndorCon = tddlDatasource.getConnection();
            otherAndorCon.setAutoCommit(false);
            Statement otherAndorPs = otherAndorCon.createStatement();
            sql = "insert into " + normaltblTableName + "(pk,id) values(" + RANDOM_ID + ",'" + RANDOM_INT + ")";
            try {
                otherAndorPs.execute(sql);
                Assert.fail();
            } catch (Exception e) {

            }
            tddlConnection.commit();
            mysqlConnection.commit();
            otherAndorPs.close();
            otherAndorPs = null;
            otherAndorCon.close();
            otherAndorCon = null;
            // 在事物提交正确查询数据
        } catch (Exception ex) {
            try {
                mysqlConnection.rollback();
                tddlConnection.rollback();
            } catch (Exception ee) {

            }
        }

        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        selectOrderAssertTranscation(sql, columnParam, null);
    }

    @Test
    public void insertRollbackTest() throws Exception {
        String sql = "insert into " + normaltblTableName + " values(?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(gmt);
        param.add(name);
        param.add(fl);

        tddlConnection.setAutoCommit(false);

        mysqlConnection.setAutoCommit(false);
        mysqlPreparedStatement = null;
        try {
            int mysqlAffectRow = mysqlUpdateDataTranscation(sql, param);
            int andorAffectRow = tddlUpdateDataTranscation(sql, param);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            // 在事物内内查到数据
            sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
            String[] columnParam = { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL" };
            selectOrderAssertTranscation(sql, columnParam, null);

            mysqlConnection.rollback();
            tddlConnection.rollback();

            // 在事物回滚查询不到数据
            selectOrderAssertTranscation(sql, columnParam, null);
        } catch (Exception ex) {
            try {
                mysqlConnection.rollback();
                tddlConnection.rollback();
            } catch (Exception ee) {

            }
        }
        // 多次回滚和提交保证不出现异常
        try {
            tddlConnection.commit();
            tddlConnection.commit();
            tddlConnection.rollback();
            tddlConnection.rollback();
        } catch (Exception e) {
            Assert.fail(ExceptionUtils.getFullStackTrace(e));
        }
    }

    @Test
    public void insertSeveralRollbackTest() throws Exception {
        String sql = "insert into " + normaltblTableName + " values(?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(gmt);
        param.add(name);
        param.add(fl);

        tddlConnection.setAutoCommit(false);
        tddlPreparedStatement = null;

        mysqlConnection.setAutoCommit(false);
        mysqlPreparedStatement = null;
        try {
            int mysqlAffectRow = mysqlUpdateDataTranscation(sql, param);
            int andorAffectRow = tddlUpdateDataTranscation(sql, param);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            // 在事物内内查到数据
            sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
            String[] columnParam = { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL" };
            selectOrderAssertTranscation(sql, columnParam, null);

            // 未提交，使用不同连接失败
            Connection otherAndorCon = tddlDatasource.getConnection();
            otherAndorCon.setAutoCommit(false);
            Statement st = otherAndorCon.createStatement();
            sql = "insert into " + normaltblTableName + "(pk,id) values(" + RANDOM_ID + ",'" + RANDOM_INT + ")";
            try {
                st.execute(sql);
                Assert.fail();
            } catch (Exception e) {
            }

            mysqlConnection.rollback();
            tddlConnection.rollback();

            st.close();
            st = null;
            otherAndorCon.close();
            otherAndorCon = null;
            sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
            // 在事物回滚查询不到数据
            selectOrderAssertTranscation(sql, columnParam, null);
        } catch (Exception ex) {
            try {
                mysqlConnection.rollback();
                tddlConnection.rollback();
            } catch (Exception ee) {

            }
        }
    }

    /**
     * 多次插入共享一个连接
     * 
     * @throws Exception
     */
    @Test
    public void insertMutilWithOneConTest() throws Exception {
        String sql = "insert into " + normaltblTableName + " values(?,?,?,?,?)";
        List<Object> param = new ArrayList<Object>();
        param.add(RANDOM_ID);
        param.add(RANDOM_INT);
        param.add(gmt);
        param.add(name);
        param.add(fl);

        List<Object> param1 = new ArrayList<Object>();
        param1.add(RANDOM_ID + 1);
        param1.add(RANDOM_INT);
        param1.add(gmt);
        param1.add(name);
        param1.add(fl);
        String[] columnParam = { "PK", "ID", "GMT_CREATE", "NAME", "FLOATCOL" };

        tddlConnection.setAutoCommit(false);
        try {

            mysqlConnection.setAutoCommit(false);

            int mysqlAffectRow = mysqlUpdateDataTranscation(sql, param);
            int andorAffectRow = tddlUpdateDataTranscation(sql, param);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            mysqlAffectRow = mysqlUpdateDataTranscation(sql, param1);
            andorAffectRow = tddlUpdateDataTranscation(sql, param1);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            // 在事物内内查到数据
            sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;

            selectOrderAssertTranscation(sql, columnParam, null);

            sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID + 1;
            selectOrderAssertTranscation(sql, columnParam, null);

            mysqlConnection.commit();
            tddlConnection.commit();
        } catch (Exception ex) {
            try {
                mysqlConnection.rollback();
                tddlConnection.rollback();
            } catch (Exception ee) {

            }
        }
        mysqlConnection.setAutoCommit(true);
        tddlConnection.setAutoCommit(true);

        // 数据提交，验证数值正确性
        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        selectOrderAssertTranscation(sql, columnParam, null);
        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID + 1;
        selectOrderAssertTranscation(sql, columnParam, null);

    }

    /**
     * 插入和查询共一个连接
     * 
     * @throws Exception
     */
    @Test
    public void insertQueryWithOneConTest() throws Exception {
        String sql = "insert into " + normaltblTableName + "(pk,id) values(" + RANDOM_ID + "," + RANDOM_INT + ")";

        tddlConnection.setAutoCommit(false);

        mysqlConnection.setAutoCommit(false);
        try {
            int mysqlAffectRow = mysqlUpdateDataTranscation(sql, null);
            int andorAffectRow = tddlUpdateDataTranscation(sql, null);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            sql = "insert into " + normaltblTableName + "(pk,id) values(" + RANDOM_ID + 1 + "," + RANDOM_INT + ")";
            mysqlAffectRow = mysqlUpdateDataTranscation(sql, null);
            andorAffectRow = tddlUpdateDataTranscation(sql, null);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            mysqlConnection.commit();
            tddlConnection.commit();
        } catch (Exception ex) {
            try {
                mysqlConnection.rollback();
                tddlConnection.rollback();
            } catch (Exception ee) {

            }
        }
        // 在事物内内查到数据
        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        String[] columnParam = { "PK", "ID" };
        selectOrderAssertTranscation(sql, columnParam, null);

        mysqlConnection.setAutoCommit(true);
        tddlConnection.setAutoCommit(true);

        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID + 1;
        selectOrderAssertTranscation(sql, columnParam, null);

        // 数据提交，验证数值正确性
        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID;
        selectOrderAssertTranscation(sql, columnParam, null);
        sql = "select * from " + normaltblTableName + " where pk=" + RANDOM_ID + 1;
        selectOrderAssertTranscation(sql, columnParam, null);

    }

    @Test
    public void updateCommitTest() throws Exception {
        long pk = 0;
        normaltblPrepare(0, 1);
        String sql = "UPDATE " + normaltblTableName + " SET id=?,gmt_create=?,name=?,floatCol=? WHERE pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(rand.nextInt());
        param.add(gmt);
        param.add("new_name" + rand.nextInt());
        param.add(fl);
        param.add(pk);

        tddlConnection.setAutoCommit(false);

        mysqlConnection.setAutoCommit(false);
        try {
            int mysqlAffectRow = mysqlUpdateDataTranscation(sql, param);
            int andorAffectRow = tddlUpdateDataTranscation(sql, param);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            mysqlConnection.commit();
            tddlConnection.commit();
        } catch (Exception ex) {
            try {
                mysqlConnection.rollback();
                tddlConnection.rollback();
            } catch (Exception ee) {

            }
        }
        sql = "select * from " + normaltblTableName + " where pk=" + pk;
        String[] columnParam = { "ID", "NAME", "FLOATCOL" };
        selectOrderAssertTranscation(sql, columnParam, null);
    }

    @Test
    public void updateRollbackTest() throws Exception {
        // TODO:ob bug，读取不到事务内的最新数据
        if (normaltblTableName.startsWith("ob")) return;

        long pk = 0l;
        normaltblPrepare(0, 1);
        String sql = "UPDATE " + normaltblTableName + " SET id=?,gmt_create=?,name=?,floatCol=? WHERE pk=?";
        List<Object> param = new ArrayList<Object>();
        param.add(rand.nextInt());
        param.add(gmt);
        param.add("new_name" + rand.nextInt());
        param.add(fl);
        param.add(pk);

        tddlConnection.setAutoCommit(false);

        mysqlConnection.setAutoCommit(false);
        try {
            int mysqlAffectRow = mysqlUpdateDataTranscation(sql, param);
            int andorAffectRow = tddlUpdateDataTranscation(sql, param);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            sql = "select * from " + normaltblTableName + " where pk=" + pk;
            String[] columnParam = { "ID", "NAME", "FLOATCOL" };
            // 没有提交验证查询的到数据
            selectOrderAssertTranscation(sql, columnParam, null);

            // 回滚
            mysqlConnection.rollback();
            tddlConnection.rollback();
        } catch (Exception ex) {
            try {
                mysqlConnection.rollback();
                tddlConnection.rollback();
            } catch (Exception ee) {

            }
        }
        // 验证查询不到数据
        sql = "select * from " + normaltblTableName + " where pk=" + pk;
        String[] columnParam1 = { "ID", "NAME", "FLOATCOL" };
        selectOrderAssertTranscation(sql, columnParam1, null);
    }

    @Test
    public void deleteCommitTest() throws Exception {
        long pk = 0l;
        normaltblPrepare(0, 1);
        String[] columnParam = { "ID", "GMT_CREATE", "NAME", "FLOATCOL" };
        String sql = "DELETE FROM " + normaltblTableName + " WHERE pk = " + pk;

        tddlConnection.setAutoCommit(false);

        mysqlConnection.setAutoCommit(false);
        try {
            int mysqlAffectRow = mysqlUpdateDataTranscation(sql, null);
            int andorAffectRow = tddlUpdateDataTranscation(sql, null);
            Assert.assertEquals(mysqlAffectRow, andorAffectRow);

            mysqlConnection.commit();
            tddlConnection.commit();
        } catch (Exception ex) {
            try {
                mysqlConnection.rollback();
                tddlConnection.rollback();
            } catch (Exception ee) {

            }
        }
        sql = "select * from " + normaltblTableName + " where pk=" + pk;

        selectOrderAssertTranscation(sql, columnParam, null);

    }

    @Test
    public void deleteRollbackTest() throws Exception {
        // TODO:ob bug，读取不到事务内的最新数据
        if (normaltblTableName.startsWith("ob")) return;

        long pk = 0l;
        normaltblPrepare(0, 1);
        String[] columnParam1 = { "ID", "NAME", "FLOATCOL" };
        selectOrderAssertTranscation("select * from " + normaltblTableName + " where pk=" + pk, columnParam1, null);

        String sql = "DELETE FROM " + normaltblTableName + " WHERE pk = " + pk;

        tddlConnection.setAutoCommit(false);

        mysqlConnection.setAutoCommit(false);

        int mysqlAffectRow = mysqlUpdateDataTranscation(sql, null);
        int andorAffectRow = tddlUpdateDataTranscation(sql, null);
        Assert.assertEquals(mysqlAffectRow, andorAffectRow);

        sql = "select * from " + normaltblTableName + " where pk=" + pk;
        String[] columnParam = { "ID", "GMT_CREATE", "NAME", "FLOATCOL" };
        // 没有提交验证查询不到数据
        selectOrderAssertTranscation(sql, columnParam, null);

        // 回滚
        mysqlConnection.rollback();
        tddlConnection.rollback();

        // 验证查询的到数据
        selectOrderAssertTranscation("select * from " + normaltblTableName + " where pk=" + pk, columnParam1, null);
    }

}
