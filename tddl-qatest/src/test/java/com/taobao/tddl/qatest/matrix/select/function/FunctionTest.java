package com.taobao.tddl.qatest.matrix.select.function;

import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.taobao.tddl.qatest.BaseMatrixTestCase;
import com.taobao.tddl.qatest.BaseTestCase;

public class FunctionTest extends BaseMatrixTestCase {

    public FunctionTest(){
        BaseTestCase.normaltblTableName = "_tddl_";
    }

    @Before
    public void prepare() throws Exception {
        demoRepoPrepare(0, 20);
    }

    @After
    public void destory() throws Exception {
        psConRcRsClose(rc, rs);
    }

    @Test
    public void absTest() throws Exception {
        String sql = "select abs(2) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select abs(-32) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void acosTest() throws Exception {
        String sql = "select acos(1) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select acos(1.0001) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select acos(0) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void atanTest() throws Exception {
        String sql = "select atan(2) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select atan(-2) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select atan(-2,2) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select atan2(pi(),0) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void ceilTest() throws Exception {
        String sql = "select ceil(1.23) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select atan(-1.23) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void convTest() throws Exception {
        String sql = "select conv('a',16,2) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select conv('6E',18,8) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select conv(-17,10,-18) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select conv(10+'10'+'10'+10,10,10) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void cosTest() throws Exception {
        String sql = "select cos(pi()) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select cos(2.2) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void cotTest() throws Exception {
        String sql = "select cot(12) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select cot(0) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void crc32Test() throws Exception {
        String sql = "select CRC32('MySQL') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select CRC32('mysql') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void degreesTest() throws Exception {
        String sql = "select DEGREES(PI()) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select DEGREES(PI()/2) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void expTest() throws Exception {
        String sql = "select EXP(2) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXP(-2) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXP(0) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void floorTest() throws Exception {
        String sql = "select FLOOR(1.23) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select FLOOR(-1.23) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void lnTest() throws Exception {
        String sql = "select ln(2) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select ln(-2) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void logTest() throws Exception {
        String sql = "select log(2) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select log(-2) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select log(2,65536) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select log(10,100) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select log(1,100) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void log2Test() throws Exception {
        String sql = "select log2(65536) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select log2(-100) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void log10Test() throws Exception {
        String sql = "select LOG10(2) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select LOG10(100) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select LOG10(-100) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void modTest() throws Exception {
        String sql = "select MOD(234, 10) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select 253 % 7 as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select MOD(29,9) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select 29 MOD 9 as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select MOD(34.5,3) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select MOD(100,0) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void piTest() throws Exception {
        String sql = "select pi() as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select PI()+0.000000000000000000 as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void powTest() throws Exception {
        String sql = "select POW(2,2) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select POWER(2,-2) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void radiansTest() throws Exception {
        String sql = "select radians(90) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select radians(45) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void roundTest() throws Exception {
        String sql = "select ROUND(-1.23) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select ROUND(-1.58) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select ROUND(1.58) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select ROUND(1.298, 1) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select ROUND(1.298, 0) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select ROUND(23.298, -1) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select ROUND(150.000,2) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select ROUND(150,2) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select ROUND(2.5) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        // sql = "select ROUND(25E-1) as a from " + normaltblTableName;
        // selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void signTest() throws Exception {
        String sql = "select SIGN(-32) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select SIGN(0) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select SIGN(234) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void sinTest() throws Exception {
        String sql = "select SIN(PI()) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select ROUND(SIN(PI())) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void sqrtTest() throws Exception {
        String sql = "select SQRT(4) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select SQRT(20) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select SQRT(-16) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void tanTest() throws Exception {
        String sql = "select TAN(PI()) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select TAN(PI()+1) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void truncateTest() throws Exception {
        String sql = "select TRUNCATE(1.223,1) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select TRUNCATE(1.999,1) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select TRUNCATE(1.999,0) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select TRUNCATE(-1.999,1) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select TRUNCATE(122,-2) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select TRUNCATE(10.28*100,0) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void caseTest() throws Exception {
        String sql = "select  CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'more' END as a from "
                     + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select CASE WHEN 1>0 THEN 'true' ELSE 'false' END as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select CASE 'b' WHEN 'a' THEN 1 WHEN 'b' THEN 2 END as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void ifTest() throws Exception {
        String sql = "select IF(1>2,2,3) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select IF(1<2,'yes','no') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select IF('test' = 'test1','no','yes') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void ifNullTest() throws Exception {
        String sql = "select IFNULL(1,0) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select IFNULL(NULL,10) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select IFNULL(1/0,10) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select IFNULL(1/0,'yes') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void nullIfTest() throws Exception {
        String sql = "select NULLIF(1,1) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select NULLIF(1,2) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select NULLIF(null,1) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select NULLIF(1,null) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void adddateTest() throws Exception {
        String sql = "select DATE_ADD('2008-01-02 00:00:00', INTERVAL 31 DAY) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select '2008-12-31 23:59:59' + INTERVAL 1 SECOND as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select INTERVAL 1 DAY + '2008-12-31 00:00:00' as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select '2005-01-01 00:00:00' - INTERVAL 1 SECOND as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select DATE_ADD('2000-12-31 23:59:59', INTERVAL 1 SECOND) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select DATE_ADD('2010-12-31 23:59:59',INTERVAL 1 DAY) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select DATE_SUB('2005-01-01 00:00:00',INTERVAL '1 1:1:1' DAY_SECOND) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select DATE_ADD('1900-01-01 00:00:00',INTERVAL '1 10' DAY_HOUR) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select DATE_SUB('1998-01-02 00:00:00', INTERVAL 31 DAY) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void daynameTest() throws Exception {
        String sql = "select DAYNAME('2007-02-03') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void dayofmonthTest() throws Exception {
        String sql = "select DAYOFMONTH('2007-02-03') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void dayofweekTest() throws Exception {
        String sql = "select DAYOFWEEK('2007-02-03') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void dayofyearTest() throws Exception {
        String sql = "select DAYOFYEAR('2007-02-03') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void fromdaysTest() throws Exception {
        String sql = "select FROM_DAYS(730669) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void hourTest() throws Exception {
        String sql = "select HOUR('10:05:03') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void lastDayTest() throws Exception {
        String sql = "select LAST_DAY('2003-02-05 00:00:00') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select LAST_DAY('2004-02-05 00:00:00') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select LAST_DAY('2004-01-01 00:00:00') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select LAST_DAY('2003-03-31 00:00:00') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void monthTest() throws Exception {
        String sql = "select MONTH('2008-02-03') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select MONTHNAME('2004-02-05 00:00:00') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void quarterTest() throws Exception {
        String sql = "select QUARTER('2008-04-01') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void secondTest() throws Exception {
        String sql = "select SECOND('10:05:03') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        // sql = "select SEC_TO_TIME(2378) as a from " + normaltblTableName;
        // selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void weekTest() throws Exception {
        String sql = "select WEEKDAY('2008-02-03 22:23:00') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select WEEKDAY('2007-11-06') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select WEEKOFYEAR('2008-02-20') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        // sql = "select WEEK('2008-02-20') as a from " + normaltblTableName;
        // selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        //
        // sql = "select SELECT WEEK('2008-02-20',0) as a from " +
        // normaltblTableName;
        // selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        //
        // sql = "select SELECT WEEK('2008-02-20',1) as a from " +
        // normaltblTableName;
        // selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        //
        // sql = "select SELECT WEEK('2008-12-31',1) as a from " +
        // normaltblTableName;
        // selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void yearTest() throws Exception {
        String sql = "select YEAR('1987-01-01') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        // sql = "select YEARWEEK('1987-01-01') as a from " +
        // normaltblTableName;
        // selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void timeToSecondsTest() throws Exception {
        String sql = "select TIME_TO_SEC('22:23:00') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select TIME_TO_SEC('00:39:38') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void timestampaddTest() throws Exception {
        String sql = "select TIMESTAMPADD(MINUTE,1,'2003-01-02 00:00:00') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select TIMESTAMPADD(WEEK,1,'2003-01-02 00:00:00') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void timestampTest() throws Exception {
        String sql = "select TIMESTAMP('2003-12-31') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        // sql = "select TIMESTAMP('2003-12-31 12:00:00','12:00:00') as a from "
        // + normaltblTableName;
        // selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        // sql = "select TIME('2003-12-31 01:02:03') as a from " +
        // normaltblTableName;
        // selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void subtimeTest() throws Exception {
        String sql = "select SUBTIME('2007-12-31 23:59:59','1:1:1') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        // sql = "select TIMESTAMP('2003-12-31 12:00:00','12:00:00') as a from "
        // + normaltblTableName;
        // selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void dataFormatTest() throws Exception {
        String sql = "select DATE_FORMAT('2009-10-04 22:23:00', '%W %M %Y') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select DATE_FORMAT('2007-10-04 22:23:00', '%H:%i:%s') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select DATE_FORMAT('1997-10-04 22:23:00','%H %k %I %r %T %S') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select DATE_FORMAT('2006-06-01', '%d') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select DATE_FORMAT('2003-10-03',GET_FORMAT(DATE,'EUR')) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select STR_TO_DATE('10.31.2003',GET_FORMAT(DATE,'USA')) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void fromUnixtimeTest() throws Exception {
        // String sql =
        // "select FROM_UNIXTIME(UNIX_TIMESTAMP(),'%Y-%d-%m %H:%i:%s') as a from "
        // + normaltblTableName;
        // String[] columnParam = { "a" };
        // selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        // sql = "select FROM_UNIXTIME(1196440219) as a from " +
        // normaltblTableName;
        // selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void makeDateTest() throws Exception {
        String sql = "select MAKEDATE(2011,31) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select MAKEDATE(2011,32) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select MAKEDATE(2011,365) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select MAKEDATE(2014,365) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select MAKEDATE(2011,0) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        // sql = "select MAKETIME(12,15,30) as a from " + normaltblTableName;
        // selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void timeFormatTest() throws Exception {
        String sql = "select TIME_FORMAT('23:00:00', '%H %k %h %I %l') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
        // sql = "select MAKETIME(12,15,30) as a from " + normaltblTableName;
        // selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void dataDiffTest() throws Exception {
        String sql = "select  DATEDIFF('2007-12-31 23:59:59','2007-12-30') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select DATEDIFF('2010-11-30 23:59:59','2010-12-31') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void peroidAddTest() throws Exception {
        String sql = "select PERIOD_ADD(200801,2) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select PERIOD_ADD(200801,-2) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void peroidDiffTest() throws Exception {
        String sql = "select PERIOD_DIFF(200802,200703) as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select PERIOD_DIFF(200703,200802) as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }

    @Test
    public void extractTTest() throws Exception {
        String sql = "SELECT EXTRACT(MICROSECOND FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXTRACT(SECOND FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXTRACT(MINUTE FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXTRACT(HOUR FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXTRACT(DAY FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        // sql = "select EXTRACT(WEEK FROM '2009-07-02 01:02:03') as a from " +
        // normaltblTableName;
        // selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXTRACT(MONTH FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXTRACT(QUARTER FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXTRACT(YEAR FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXTRACT(SECOND_MICROSECOND FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXTRACT(MINUTE_MICROSECOND FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXTRACT(MINUTE_SECOND FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXTRACT(HOUR_MICROSECOND FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXTRACT(HOUR_SECOND FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXTRACT(HOUR_MINUTE FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXTRACT(DAY_MICROSECOND FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXTRACT(DAY_SECOND FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXTRACT(DAY_MINUTE FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXTRACT(DAY_HOUR FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

        sql = "select EXTRACT(YEAR_MONTH FROM '2009-07-02 01:02:03') as a from " + normaltblTableName;
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);
    }
}
