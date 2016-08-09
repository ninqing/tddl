package com.taobao.tddl.qatest.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.junit.Assert;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.matrix.jdbc.TDataSource;

public class Validator {

    public static TDataSource tddlDatasource;
    public static Connection  mysqlConnection;
    public static Connection  tddlConnection;
    public PreparedStatement  mysqlPreparedStatement;
    public PreparedStatement  tddlPreparedStatement;

    /**
     * mysql查询数据
     * 
     * @param sql
     * @param param
     * @return
     * @throws Exception
     */
    public ResultSet mysqlQueryData(String sql, List<Object> param) throws Exception {
        ResultSet rs = null;
        try {
            mysqlPreparedStatement = mysqlConnection.prepareStatement(sql);
            if (param == null) {
                rs = mysqlPreparedStatement.executeQuery();
            } else {
                for (int i = 0; i < param.size(); i++) {
                    mysqlPreparedStatement.setObject(i + 1, param.get(i));
                }
                rs = mysqlPreparedStatement.executeQuery();
                // //Thread.sleep(500);
            }
        } catch (Exception ex) {
            throw new Exception(ex);
        } finally {

        }
        return rs;
    }

    /**
     * mysql查询数据
     * 
     * @param sql
     * @param param
     * @return
     * @throws Exception
     */
    public ResultSet andorQueryData(String sql, List<Object> param) throws Exception {
        ResultSet rs = null;
        try {
            tddlPreparedStatement = tddlConnection.prepareStatement(sql);
            if (param == null) {
                rs = tddlPreparedStatement.executeQuery();
            } else {
                for (int i = 0; i < param.size(); i++) {
                    tddlPreparedStatement.setObject(i + 1, param.get(i));
                }
                rs = tddlPreparedStatement.executeQuery();
            }
        } catch (Exception ex) {
            // tddlCon.rollback();
            if (!tddlConnection.getAutoCommit()) {
                tddlConnection.rollback();
            }
            throw new Exception(ex);
        } finally {

        }
        return rs;
    }

    /**
     * 返回结果集的条数
     * 
     * @param rs
     * @return
     * @throws SQLException
     */
    public int resultsSize(ResultSet rs) throws SQLException {
        int row = 0;
        while (rs.next()) {
            row++;
        }
        return row;
    }

    /**
     * 验证andor和mysql数据的结果集包含的内容相同，不保证顺序
     * 
     * @param rs
     * @param ret
     * @param columnParam
     * @throws Exception
     */
    public void assertContentSame(ResultSet rs, ResultSet ret, String[] columnParam) throws Exception {
        boolean same = false;
        List<Object> mutilMysqlResult = new ArrayList<Object>();
        List<Object> mutilResult = new ArrayList<Object>();
        try {

            while (rs.next()) {
                List<Object> mysqlResult = new ArrayList<Object>();
                for (int i = 0; i < columnParam.length; i++) {
                    mysqlResult.add(getObject(rs, columnParam, i));
                }
                mutilMysqlResult.add(mysqlResult);
            }

            while (ret.next()) {
                List<Object> result = new ArrayList<Object>();
                for (int i = 0; i < columnParam.length; i++) {
                    result.add(getObject(ret, columnParam, i));
                }
                mutilResult.add(result);
            }
            if (mutilMysqlResult.size() != mutilResult.size()) {
                Assert.fail();
            }
            for (int i = 0; i < mutilMysqlResult.size(); i++) {
                Object mResult = mutilMysqlResult.get(i);
                mutilResult.contains(mResult);
                mutilResult.remove(mResult);
            }

            if (mutilResult.size() == 0 && ret.next() == false) {
                same = true;
            }
            if (same != true) {
                Assert.assertEquals(mutilMysqlResult, mutilResult);
            }
        } finally {
            rs.close();
            ret.close();
        }
    }

    /**
     * 验证andor和mysql数据的结果集包含的内容相同，不保证顺序
     * 
     * @param rs
     * @param ret
     * @param columnParam
     * @throws Exception
     */
    public void assertContentSameByIndex(ResultSet rs, ResultSet ret, int columnParam) throws Exception {
        boolean same = false;
        List<Object> mutilMysqlResult = new ArrayList<Object>();
        List<Object> mutilResult = new ArrayList<Object>();
        try {
            while (rs.next()) {
                List<Object> mysqlResult = new ArrayList<Object>();
                List<Object> Result = new ArrayList<Object>();
                ret.next();
                for (int i = 1; i < columnParam; i++) {
                    mysqlResult.add(rs.getObject(i));
                    Result.add(rs.getObject(i));
                }
                mutilMysqlResult.add(mysqlResult);
                mutilResult.add(Result);
            }
            if (mutilMysqlResult.size() != mutilResult.size()) {
                Assert.fail();
            }
            for (int i = 0; i < mutilMysqlResult.size(); i++) {
                Object mResult = mutilMysqlResult.get(i);
                mutilResult.contains(mResult);
                mutilResult.remove(mResult);
            }
            if (mutilResult.size() == 0 && ret.next() == false) {
                same = true;
            }
            if (same != true) {
                Assert.fail("Results not same!");
            }
        } finally {
            rs.close();
            ret.close();
        }
    }

    /**
     * 验证andor和mysql数据的结果集包含的内容相同，保证顺序
     * 
     * @param rs
     * @param ret
     * @param columnParam
     * @throws Exception
     */
    public void assertOrder(ResultSet rs, ResultSet ret, String[] columnParam) throws Exception {
        try {
            while (rs.next() == true) {
                ret.next();
                List<Object> mysqlResult = new ArrayList<Object>();
                List<Object> Result = new ArrayList<Object>();
                for (int i = 0; i < columnParam.length; i++) {
                    Object mysqlData = getObject(rs, columnParam, i);
                    mysqlResult.add(mysqlData);
                    mysqlData = getObject(ret, columnParam, i);
                    Result.add(mysqlData);
                }
                Assert.assertEquals(mysqlResult, Result);
            }
            Assert.assertFalse(ret.next());
        } finally {
            rs.close();
            ret.close();
        }
    }

    /**
     * 验证andor和mysql 中orderBy中的字段不是主键，验证对应的字段保持一致
     * 
     * @param rs
     * @param ret
     * @param columnParam
     * @throws Exception
     */
    public void assertOrderNotKeyCloumn(ResultSet rs, ResultSet ret, String[] columnParam, String notKeyCloumn)
                                                                                                               throws Exception {
        try {
            boolean same = false;
            Object orderClounmValue = null;
            Object nextClounmValue = null;
            List<Object> mutilMysqlResult = new ArrayList<Object>();
            List<Object> mutilResult = new ArrayList<Object>();
            while (rs.next() == true) {
                ret.next();
                List<Object> mysqlResult = new ArrayList<Object>();
                List<Object> result = new ArrayList<Object>();

                nextClounmValue = rs.getObject(notKeyCloumn);
                if (nextClounmValue.equals(orderClounmValue) || rs.isFirst()) {
                    for (int i = 0; i < columnParam.length; i++) {
                        if (rs.isFirst()) {
                            orderClounmValue = rs.getObject(notKeyCloumn);
                        }
                        Object mysqlData = getObject(rs, columnParam, i);
                        mysqlResult.add(mysqlData);
                        mysqlData = getObject(ret, columnParam, i);
                        result.add(mysqlData);
                        orderClounmValue = rs.getObject(notKeyCloumn);
                    }
                    mutilMysqlResult.add(mysqlResult);
                    mutilResult.add(result);
                } else {
                    if (mutilMysqlResult.size() != mutilResult.size()) {
                        Assert.fail();
                    }
                    for (int i = 0; i < mutilMysqlResult.size(); i++) {
                        Object mResult = mutilMysqlResult.get(i);
                        mutilResult.contains(mResult);
                        mutilResult.remove(mResult);
                    }
                    if (mutilResult.size() == 0) same = true;
                    Assert.assertEquals(true, same);
                    for (int i = 0; i < columnParam.length; i++) {
                        Object mysqlData = getObject(rs, columnParam, i);
                        mysqlResult.add(mysqlData);
                        mysqlData = getObject(ret, columnParam, i);
                        result.add(mysqlData);
                        orderClounmValue = rs.getObject(notKeyCloumn);
                    }
                    mutilMysqlResult.clear();
                    mutilResult.clear();
                    mutilMysqlResult.add(mysqlResult);
                    mutilResult.add(result);
                }
            }
            Assert.assertFalse(ret.next());
        } finally {
            rs.close();
            ret.close();
        }
    }

    /**
     * 验证andor和mysql数据的结果集包含的内容相同，保证顺序(事物特殊处理，最后不关闭数据库连接)
     * 
     * @param rs
     * @param ret
     * @param columnParam
     * @throws Exception
     */
    public void assertOrderTransaction(ResultSet rs, ResultSet ret, String[] columnParam) throws Exception {
        try {
            while (rs.next() == true) {
                ret.next();
                List<Object> mysqlResult = new ArrayList<Object>();
                List<Object> result = new ArrayList<Object>();
                for (int i = 0; i < columnParam.length; i++) {

                    mysqlResult.add(getObject(rs, columnParam, i));
                    // mysqlResult.add(rs.getObject(columnParam[i]));
                    // result.add(ret.getObject(columnParam[i]));

                    result.add(getObject(ret, columnParam, i));

                }
                Assert.assertEquals(mysqlResult, result);
            }
            Assert.assertFalse(ret.next());
        } finally {

            rs.close();
            ret.close();
        }
    }

    public static class MyNumber {

        BigDecimal number;

        public MyNumber(BigDecimal number){
            super();
            this.number = number;
        }

        @Override
        public String toString() {
            return this.number == null ? null : this.number.toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) {
                throw new RuntimeException("类型不一致" + this.getClass() + "  " + obj.getClass());
            }
            MyNumber other = (MyNumber) obj;
            if (number == null) {
                if (other.number != null) return false;
            } else {
                BigDecimal o = this.number;
                BigDecimal o2 = other.number;

                if (o.subtract(o2).abs().compareTo(new BigDecimal(0.1)) < 0) {
                    return true;
                } else return false;
            }
            return true;
        }
    }

    public Object getObject(ResultSet rs, String[] columnParam, int i) throws SQLException {
        Object data = rs.getObject(columnParam[i]);
        if (data instanceof Long) {
            data = new BigDecimal((Long) data);
        } else if (data instanceof Short) {
            data = new BigDecimal((Short) data);
        } else if (data instanceof Integer) {
            data = new BigDecimal((Integer) data);
        } else if (data instanceof Float) {
            data = new BigDecimal((Float) data);
        } else if (data instanceof Double) {
            data = new BigDecimal((Double) data);
        } else if (data instanceof BigDecimal) {
            // data = data;
        } else if (data instanceof Date) {
            data = ((Date) data).getTime() / 1000;
        } else if (data instanceof byte[]) {
            data = rs.getString(columnParam[i]);
        } else if (data instanceof BigInteger) {
            data = new BigDecimal((BigInteger) data);
        }

        if (data instanceof BigDecimal) {
            data = new MyNumber((BigDecimal) data);
        }
        return data;
    }

    /**
     * mysql更新数据
     * 
     * @param sql
     * @param param
     * @return
     * @throws Exception
     */
    public int mysqlUpdateDataBatch(String sql, List<List<Object>> params) throws Exception {
        int nn[];
        try {
            mysqlPreparedStatement = mysqlConnection.prepareStatement(sql);
            if (params == null) {
                return mysqlPreparedStatement.executeUpdate();
            } else {

                for (List<Object> param : params) {
                    for (int i = 0; i < param.size(); i++) {
                        mysqlPreparedStatement.setObject(i + 1, param.get(i));
                    }
                    mysqlPreparedStatement.addBatch();
                }
                nn = mysqlPreparedStatement.executeBatch();

                int rs = 0;
                for (int n : nn) {
                    rs += n;
                }

                return rs;
            }

        } catch (Exception ex) {
            throw new TddlRuntimeException(ex);
        } finally {

        }
    }

    /**
     * Andor更新数据
     * 
     * @param sql
     * @param param
     * @return
     * @throws Exception
     */
    public int tddlUpdateDataBatch(String sql, List<List<Object>> params) throws Exception {
        int nn[];
        try {
            tddlPreparedStatement = tddlConnection.prepareStatement(sql);
            if (params == null) {
                return tddlPreparedStatement.executeUpdate();
            } else {
                for (List<Object> param : params) {
                    for (int i = 0; i < param.size(); i++) {
                        tddlPreparedStatement.setObject(i + 1, param.get(i));
                    }
                    tddlPreparedStatement.addBatch();
                }
                nn = tddlPreparedStatement.executeBatch();

                int rs = 0;
                for (int n : nn) {
                    rs += n;
                }

                return rs;
            }

        } catch (Exception ex) {
            throw new TddlRuntimeException(ex);
        } finally {
        }
    }

    /**
     * mysql更新数据
     * 
     * @param sql
     * @param param
     * @return
     * @throws Exception
     */
    public int mysqlUpdateData(String sql, List<Object> param) throws Exception {
        int rs = 0;
        try {
            mysqlPreparedStatement = mysqlConnection.prepareStatement(sql);
            if (param == null) {
                rs = mysqlPreparedStatement.executeUpdate();
            } else {

                for (int i = 0; i < param.size(); i++) {
                    mysqlPreparedStatement.setObject(i + 1, param.get(i));
                }
                rs = mysqlPreparedStatement.executeUpdate();
            }

        } catch (Exception ex) {
            throw new TddlRuntimeException(ex);
        } finally {

        }
        return rs;
    }

    /**
     * mysql事物更新数据
     * 
     * @param sql
     * @param param
     * @return
     * @throws Exception
     */
    public int mysqlUpdateDataTranscation(String sql, List<Object> param) throws Exception {
        int rs = 0;
        try {
            mysqlPreparedStatement = mysqlConnection.prepareStatement(sql);
            if (param == null) {
                rs = mysqlPreparedStatement.executeUpdate();
            } else {
                for (int i = 0; i < param.size(); i++) {
                    if (param.get(i) instanceof java.util.Date) {
                        param.set(i, DateUtil.formatDate((java.util.Date) param.get(i), DateUtil.DATE_FULLHYPHEN));
                    }
                }
                for (int i = 0; i < param.size(); i++) {
                    mysqlPreparedStatement.setObject(i + 1, param.get(i));
                }
                rs = mysqlPreparedStatement.executeUpdate();
            }

        } catch (Exception ex) {
            throw new TddlRuntimeException(ex);
        } finally {
        }
        return rs;
    }

    /**
     * tddl更新数据
     * 
     * @param sql
     * @param param
     * @return
     * @throws Exception
     */
    public int tddlUpdateData(String sql, List<Object> param) throws Exception {
        int rs = 0;
        try {
            tddlPreparedStatement = tddlConnection.prepareStatement(sql);
            if (param == null) {
                rs = tddlPreparedStatement.executeUpdate();
            } else {
                for (int i = 0; i < param.size(); i++) {

                    if (param.get(i) == null) {
                        tddlPreparedStatement.setNull(i + 1, java.sql.Types.NULL);
                    } else {
                        tddlPreparedStatement.setObject(i + 1, param.get(i));
                    }
                }
                rs = tddlPreparedStatement.executeUpdate();
            }

        } catch (Exception ex) {
            throw new TddlRuntimeException(ex);
        } finally {
        }
        return rs;
    }

    /**
     * Andor事物更新数据
     * 
     * @param sql
     * @param param
     * @return
     * @throws Exception
     */
    public int tddlUpdateDataTranscation(String sql, List<Object> param) throws Exception {
        int rs = 0;
        try {
            tddlPreparedStatement = tddlConnection.prepareStatement(sql);
            if (param == null) {
                rs = tddlPreparedStatement.executeUpdate();
            } else {
                for (int i = 0; i < param.size(); i++) {
                    tddlPreparedStatement.setObject(i + 1, param.get(i));
                }
                rs = tddlPreparedStatement.executeUpdate();
            }

        } catch (Exception ex) {
            throw new TddlRuntimeException(ex);
        } finally {
        }
        return rs;
    }

    /**
     * 通过msyql和andor操作数据，验证最终数据影响条数一致
     * 
     * @param sql
     * @param param
     * @throws Exception
     */
    public void executeCountAssert(String sql, List<Object> param) throws Exception {
        int andorAffectRow = tddlUpdateData(sql, param);
        int mysqlAffectRow = mysqlUpdateData(sql, param);
        Assert.assertEquals(mysqlAffectRow, andorAffectRow);
    }

    /**
     * msyql和andor同时操作数据
     * 
     * @param sql
     * @param param
     * @throws Exception
     */
    public void execute(String sql, List<Object> param) throws Exception {
        mysqlUpdateData(sql, param);
        tddlUpdateData(sql, param);
    }

    /**
     * msyql和andor同时操作数据
     * 
     * @param sql
     * @param param
     * @throws Exception
     */
    public void executeBatch(String sql, List<List<Object>> param) throws Exception {
        mysqlUpdateDataBatch(sql, param);
        tddlUpdateDataBatch(sql, param);
    }

    /**
     * msyql和andor同时查询数据，验证andor的最终结果和msyql的最终结果顺序一致
     * 
     * @param sql
     * @param columnParam
     * @param param
     * @throws Exception
     */
    public void selectOrderAssert(String sql, String[] columnParam, List<Object> param) throws Exception {
        ResultSet rs = mysqlQueryData(sql, param);
        ResultSet rc = andorQueryData(sql, param);
        assertOrder(rs, rc, columnParam);
    }

    /**
     * mysql和andor同时查询数据，orderBy中的字段不是主键，验证对应的字段保持一致
     * 
     * @param sql
     * @param columnParam
     * @param param
     * @throws Exception
     */
    public void selectOrderAssertNotKeyCloumn(String sql, String[] columnParam, List<Object> param, String notKeyCloumn)
                                                                                                                        throws Exception {
        ResultSet rs = mysqlQueryData(sql, param);
        ResultSet rc = andorQueryData(sql, param);
        assertOrderNotKeyCloumn(rs, rc, columnParam, notKeyCloumn);

    }

    /**
     * msyql和andor同时查询数据，验证andor的最终结果和msyql的最终结果顺序一致
     * 
     * @param sql
     * @param columnParam
     * @param param
     * @throws Exception
     */
    public void selectOrderAssertTranscation(String sql, String[] columnParam, List<Object> param) throws Exception {
        ResultSet rs = mysqlQueryData(sql, param);
        ResultSet rc = andorQueryData(sql, param);
        assertOrderTransaction(rs, rc, columnParam);
    }

    /**
     * msyql和andor同时查询数据，验证andor的最终结果和msyql的最终结果集是一致的，不用保证顺序
     * 
     * @param sql
     * @param columnParam
     * @param param
     * @throws Exception
     */
    public void selectContentSameAssert(String sql, String[] columnParam, List<Object> param) throws Exception {
        ResultSet rs = null;
        ResultSet rc = null;
        try {
            rs = mysqlQueryData(sql, param);
            rc = andorQueryData(sql, param);
            assertContentSame(rs, rc, columnParam);
        } finally {
            rsRcClose(rs, rc);
        }
    }

    /**
     * msyql和andor同时查询数据，验证andor的最终结果和msyql的最终结果集是一致的，不用保证顺序
     * 
     * @param sql
     * @param columnParam
     * @param param
     * @throws Exception
     */
    public void selectContentSameAssertByIndex(String sql, int columnParam, List<Object> param) throws Exception {
        ResultSet rs = null;
        ResultSet rc = null;
        try {
            rs = mysqlQueryData(sql, param);
            rc = andorQueryData(sql, param);
            assertContentSameByIndex(rs, rc, columnParam);
        } finally {
            rsRcClose(rs, rc);
        }
    }

    /**
     * rc,rs结果集的关闭
     * 
     * @param rs
     * @param rc
     * @throws SQLException
     */

    public void rsRcClose(ResultSet rs, ResultSet rc) throws SQLException {
        if (rs != null) {
            rs.close();
            rs = null;
        }
        if (rc != null) {
            rc.close();
            rc = null;
        }
    }

    /**
     * msyql和andor同时查询数据，验证取出的数据条数一致（一般用于limit验证中）
     * 
     * @param sql
     * @param param
     * @throws Exception
     * @throws SQLException
     */
    public void selectConutAssert(String sql, List<Object> param) throws Exception, SQLException {
        ResultSet rs = null;
        ResultSet rc = null;
        try {
            rc = andorQueryData(sql, param);
            rs = mysqlQueryData(sql, param);
            Assert.assertEquals(resultsSize(rs), resultsSize(rc));
        } finally {

            rsRcClose(rs, rc);
        }
    }

    /**
     * 在有别名情况下的验证
     * 
     * @param sql
     * @param columnParam
     * @param tabelName
     * @param param
     * @throws Exception
     */
    public void assertAlias(String sql, String[] columnParam, String tabelName, List<Object> param) throws Exception {
        ResultSet rc = andorQueryData(sql, param);
        ResultSet rs = mysqlQueryData(sql, param);
        try {
            while (rs.next() == true) {
                List<Object> mysqlResult = new ArrayList<Object>();
                List<Object> result = new ArrayList<Object>();
                if (!rc.next()) {
                    Assert.assertTrue("长度不够", false);
                }
                for (int i = 0; i < columnParam.length; i++) {
                    Object obj = getObject(rs, columnParam, i);
                    mysqlResult.add(obj);
                    obj = getObject(rc, columnParam, i);
                    result.add(obj);
                }
                Assert.assertEquals(mysqlResult, result);
            }
            Assert.assertFalse(rc.next());
        } finally {
            rsRcClose(rs, rc);
        }
    }

    /**
     * ps,con,rc,rs的关闭
     * 
     * @param rc
     * @param mysqlPreparedStatement
     * @param mysqlConnection
     * @param rs
     * @throws SQLException
     */

    public void psConRcRsClose(ResultSet rc, ResultSet rs) throws SQLException {
        if (mysqlPreparedStatement != null) {
            mysqlPreparedStatement.close();
            mysqlPreparedStatement = null;
        }
        if (tddlPreparedStatement != null) {
            tddlPreparedStatement.close();
            tddlPreparedStatement = null;
        }

        if (rc != null) {
            rc.close();
            rc = null;
        }
        if (rs != null) {
            rs.close();
            rs = null;
        }
    }

    /**
     * 日期比较函数
     * 
     * @param cal
     * @param cal1
     */

    public void dataAssert(Calendar cal, Calendar cal1) {
        Assert.assertEquals(cal.get(Calendar.YEAR), cal1.get(Calendar.YEAR));
        Assert.assertEquals(cal.get(Calendar.MONTH), cal1.get(Calendar.MONTH));
        Assert.assertEquals(cal.get(Calendar.DATE), cal1.get(Calendar.DATE));
    }

    /**
     * ResultCursor ,ResultSet的关闭
     * 
     * @throws Exception
     * @throws SQLException
     */

    public void rcRsDestory(ResultSet rc, ResultSet rs) throws Exception, SQLException {
        if (rc != null) {
            rc.close();
            rc = null;
        }
        if (rs != null) {
            rs.close();
            rs = null;
        }
    }

    /**
     * 建立mysql连接
     * 
     * @return
     */
    public static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://10.232.24.104:3306/andor_qatest";
            String user = "diamond";
            String passWord = "diamond";
            conn = DriverManager.getConnection(url, user, passWord);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }
}
