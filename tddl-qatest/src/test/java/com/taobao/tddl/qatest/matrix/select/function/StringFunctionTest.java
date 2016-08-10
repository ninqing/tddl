package com.taobao.tddl.qatest.matrix.select.function;

import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.taobao.tddl.qatest.BaseMatrixTestCase;

public class StringFunctionTest extends BaseMatrixTestCase {

    public StringFunctionTest(){
    }

    @Before
    public void prepare() throws Exception {
        demoRepoPrepare(0, 20);
        // tddlUpdateData("delete from _tddl_", null);

    }

    @After
    public void destory() throws Exception {
        psConRcRsClose(rc, rs);
    }

    @Test
    public void concatTest() throws Exception {
        String sql = " select cast(11 as unsigned int) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void concatTest2() throws Exception {
        String sql = "select concat('aa', null, 'cc') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void concatTest3() throws Exception {
        String sql = "select concat('aa', name, 'cc') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void concatTest4() throws Exception {
        String sql = "select concat(14.3) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void concatWSTest() throws Exception {
        String sql = "select concat_ws('-','aa', 'bb', 'cc') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void concatWSTest2() throws Exception {
        String sql = "select concat_ws('-','aa', null, 'cc') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void concatWSTest3() throws Exception {
        String sql = "select concat_ws('-','aa', name, 'cc') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void concatWSTest4() throws Exception {
        String sql = "select concat_ws(null,'aa', 'bb', 'cc') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void binTest() throws Exception {
        String sql = "select bin(12) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void binTest1() throws Exception {
        String sql = "select bin(null) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void bitLengthTest() throws Exception {
        String sql = "select bit_length('text') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void bitLengthTest1() throws Exception {
        String sql = "select bit_length(null) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void asciiTest() throws Exception {
        String sql = "select ascii(2) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void asciiTest1() throws Exception {
        String sql = "select ascii('2') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void asciiTest2() throws Exception {
        String sql = "select ascii('dx') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void asciiTest3() throws Exception {
        String sql = "select ascii(null) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void asciiTest4() throws Exception {
        String sql = "select ascii(name) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void eltTest() throws Exception {
        String sql = "select elt(2,name,'ss','sun') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void eltTest2() throws Exception {
        String sql = "select elt(null,name,'ss','sun') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void eltTest3() throws Exception {
        String sql = "select elt(-1,name,'ss','sun') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void eltTest4() throws Exception {
        String sql = "select elt(3,name,'ss','sun') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void eltTest5() throws Exception {
        String sql = "select elt(4,name,'ss','sun') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void eltTest6() throws Exception {
        String sql = "select elt(3,name,'ss',null) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void eltTest7() throws Exception {
        String sql = "select elt(1,2,'ss',null) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void fieldTest() throws Exception {
        String sql = "select field('ej', 'Hej', 'ej', 'ej', 'hej', 'foo') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void fieldTest2() throws Exception {
        String sql = "select field('e', 'Hej', 'ej', 'ej', 'hej', 'foo') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void fieldTest3() throws Exception {
        String sql = "select field(null, 'Hej', 'ej', 'ej', null, 'foo') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void findInSetTest() throws Exception {
        String sql = "select FIND_IN_SET('b','a,b,c,d') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void findInSetTest1() throws Exception {
        String sql = "select FIND_IN_SET(null,'a,b,c,d') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void findInSetTest2() throws Exception {
        String sql = "select FIND_IN_SET('a',null) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void findInSetTest3() throws Exception {
        String sql = "select FIND_IN_SET('ef','a,b,c,de') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void hexTest() throws Exception {
        String sql = "select HEX('abc') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Ignore
    @Test
    public void hexTest2() throws Exception {
        String sql = "select 0x616263 a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void hexTest3() throws Exception {
        String sql = "select HEX(255) a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void hexTest4() throws Exception {
        String sql = "select CONV(HEX(255),16,10) a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void insertTest() throws Exception {
        String sql = "select INSERT('Quadratic', 3, 4, 'What') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void insertTest1() throws Exception {
        String sql = "select INSERT('Quadratic', -1, 4, 'What') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void insertTest2() throws Exception {
        String sql = "select INSERT('Quadratic', 3, 5, 'What') a ,INSERT('Quadratic', 3,6, 'What') b,INSERT('Quadratic', 3,7, 'What') c from _tddl_";
        String[] columnParam = { "a", "b", "c" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void instrTest() throws Exception {
        String sql = "select INSTR('foobarbar', 'bar') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void instrTest1() throws Exception {
        String sql = "select INSTR('xbar', 'foobar') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void lcaseTest() throws Exception {
        String sql = "select lcase('QUADRATICALLY') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void lowerTest() throws Exception {
        String sql = "select lower('QUADRATICALLY') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void ucaseTest() throws Exception {
        String sql = "select ucase('Hej') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void upperTest() throws Exception {
        String sql = "select upper('Hej') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void leftTest() throws Exception {
        String sql = "select LEFT('foobarbar', 5) a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void rightTest() throws Exception {
        String sql = "select right('foobarbar', 5) a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void leftTest1() throws Exception {
        String sql = "select LEFT('foobarbar', -1) a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void ltrimTest() throws Exception {
        String sql = "select LTRIM('  barbar') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void rtrimTest() throws Exception {
        String sql = "select RTRIM('barbar   ') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void lpadTest() throws Exception {
        String sql = "select LPAD('hi',4,'??') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void lpadTest2() throws Exception {
        String sql = "select LPAD('hi',3,'??') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void lpadTest3() throws Exception {
        String sql = "select LPAD('hi',2,'??') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void lpadTest4() throws Exception {
        String sql = "select LPAD('hi',1,'??') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void lpadTest5() throws Exception {
        String sql = "select LPAD('hi',-1,'??') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void rpadTest() throws Exception {
        String sql = "select RPAD('hi',4,'??') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void rpadTest2() throws Exception {
        String sql = "select RPAD('hi',3,'??') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void rpadTest3() throws Exception {
        String sql = "select RPAD('hi',2,'??') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void rpadTest4() throws Exception {
        String sql = "select RPAD('hi',1,'??') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void rpadTest5() throws Exception {
        String sql = "select RPAD('hi',-1,'??') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void strCmpTest() throws Exception {
        String sql = "select STRCMP('text', 'text2') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void strCmpTest1() throws Exception {
        String sql = "select STRCMP('text2', 'text2') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void strCmpTest2() throws Exception {
        String sql = "select STRCMP('text2', 'text') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void strCmpTest3() throws Exception {
        String sql = "select STRCMP(null, 'text') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void spaceTest() throws Exception {
        String sql = "select space(5) a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void spaceTest1() throws Exception {
        String sql = "select space(-1) a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void reverseTest() throws Exception {
        String sql = "select reverse('abc') a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void repeatTest() throws Exception {
        String sql = "select REPEAT('MySQL', 3)  a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void repeatTest1() throws Exception {
        String sql = "select REPEAT('MySQL', 0)  a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void octTest() throws Exception {
        String sql = "select oct(12) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void octTest1() throws Exception {
        String sql = "select oct(null) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void substringTest() throws Exception {
        String sql = "select SUBSTRING('Quadratically',5) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void substringTest1() throws Exception {
        String sql = "select SUBSTRING('foobarbar' FROM 4) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void substringTest2() throws Exception {
        String sql = "select SUBSTRING('Quadratically',5,6) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void substringTest3() throws Exception {
        String sql = "select SUBSTRING('Sakila', -3) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void substringTest4() throws Exception {
        String sql = "select SUBSTRING('Sakila', -5, 3) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void substringTest5() throws Exception {
        String sql = "select SUBSTRING('Sakila' FROM -4 FOR 2) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void substrTest5() throws Exception {
        String sql = "select SUBSTR('Sakila' FROM -4 FOR 2) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void subStringIndexTest() throws Exception {
        String sql = "select SUBSTRING_INDEX('www.mysql.com', '.', 2) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void subStringIndexTest1() throws Exception {
        String sql = "select SUBSTRING_INDEX('www.mysql.com', '.', 1) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void subStringIndexTest2() throws Exception {
        String sql = "select SUBSTRING_INDEX('www.mysql.com', '.', 0) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void subStringIndexTest3() throws Exception {
        String sql = "select SUBSTRING_INDEX('www.mysql.com', '.', 3) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void subStringIndexTest4() throws Exception {
        String sql = "select SUBSTRING_INDEX('www.mysql.com', '/', 1) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void subStringIndexTest5() throws Exception {
        String sql = "select SUBSTRING_INDEX('www..mysql..com', '..', 2) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void subStringIndexTest6() throws Exception {
        String sql = "select SUBSTRING_INDEX('www..mysql..com', '..', 1) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void subStringIndexTest7() throws Exception {
        String sql = "select SUBSTRING_INDEX('www..mysql..com', '..', 0) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void subStringIndexTest8() throws Exception {
        String sql = "select SUBSTRING_INDEX('www..mysql..com', '..', 3) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void subStringIndexTest9() throws Exception {
        String sql = "select SUBSTRING_INDEX('www.mysql.com', '//', 1) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void subStringIndexTest10() throws Exception {
        String sql = "select SUBSTRING_INDEX('www.mysql.com', '.', -2) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void subStringIndexTest11() throws Exception {
        String sql = "select SUBSTRING_INDEX('www.mysql.com', '.', -1) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void subStringIndexTest12() throws Exception {
        String sql = "select SUBSTRING_INDEX('www.mysql.com', '.', -3) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void subStringIndexTest13() throws Exception {
        String sql = "select SUBSTRING_INDEX('www.mysql.com', 'null', -3) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void midTest() throws Exception {
        String sql = "select mid('www.mysql.com', '.', 1) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void locateTest() throws Exception {
        String sql = "select LOCATE('bar', 'foobarbar') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void locateTest1() throws Exception {
        String sql = "select LOCATE('xbar', 'foobar') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void locateTest2() throws Exception {
        String sql = "select LOCATE('bar', 'foobarbar', 5) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void locateTest3() throws Exception {
        String sql = "select LOCATE('xbar', null) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void locateTest4() throws Exception {
        String sql = "select LOCATE(null, 'foobar') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void positionTest() throws Exception {
        String sql = "select position('xbar' in 'foobar') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void unhexTest() throws Exception {
        String sql = "select UNHEX('4D7953514C') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void unhexTest1() throws Exception {
        String sql = "select UNHEX('0x4D7953514C') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void unhexTest2() throws Exception {
        String sql = "select UNHEX(' 4D7953514C') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void unhexTest3() throws Exception {
        String sql = "select UNHEX(HEX('string')) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void unhexTest4() throws Exception {
        String sql = "select HEX(UNHEX('1267')) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void unhexTest5() throws Exception {
        String sql = "select unhex('GG') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void unhexTest6() throws Exception {
        String sql = "select unhex('1') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void trimTest() throws Exception {
        String sql = "select TRIM('  bar   ') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void trimTest1() throws Exception {
        String sql = "select TRIM(TRAILING  'x' FROM 'xxxbarxxx') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void trimTest2() throws Exception {
        String sql = "select TRIM(LEADING  'x' FROM 'xxxbarxxx') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void trimTest3() throws Exception {
        String sql = "select TRIM(BOTH  'x' FROM 'xxxbarxxx') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void trimTest4() throws Exception {
        String sql = "select TRIM('x' FROM 'xxxbarxxx') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void quoteTest() throws Exception {
        String sql = "select QUOTE('Don\\'t!') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void quoteTest1() throws Exception {
        String sql = "select QUOTE(\"Don't!\") as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void quoteTest2() throws Exception {
        String sql = "select QUOTE(null) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void ordTest() throws Exception {
        String sql = "select ORD('sun') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void replaceTest() throws Exception {
        String sql = "select REPLACE('www.mysql.com', 'w', 'Ww') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void formatTest() throws Exception {
        String sql = "select FORMAT(12332.123456, 4) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void formatTest1() throws Exception {
        String sql = "select FORMAT(12332.1,4) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void formatTest2() throws Exception {
        String sql = "select FORMAT(12332.2,0) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void exportSetTest() throws Exception {
        String sql = "select EXPORT_SET(5,'Y','N',',',4) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void exportSetTest1() throws Exception {
        String sql = "select EXPORT_SET(6,'1','0',',',10) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void exportSetTest2() throws Exception {
        String sql = "select EXPORT_SET(254,'1','0',',',5) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void exportSetTest3() throws Exception {
        String sql = "select EXPORT_SET(6,'11','0',',',10) as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void makeSetTest() throws Exception {
        String sql = "select MAKE_SET(1,'a','b','c') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void makeSetTest1() throws Exception {
        String sql = "select MAKE_SET(1 | 4,'hello','nice','world') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void makeSetTest2() throws Exception {
        String sql = "select MAKE_SET(1 | 4,'hello','nice',NULL,'world') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void makeSetTest3() throws Exception {
        String sql = "select MAKE_SET(0,'a','b','c') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void makeSetTest4() throws Exception {
        String sql = "select MAKE_SET(6,'hello','nice',NULL,'world','yeah') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

    @Test
    public void makeSetTest5() throws Exception {
        String sql = "select MAKE_SET(6,'hello','nice','world') as a from _tddl_";
        String[] columnParam = { "a" };
        selectContentSameAssert(sql, columnParam, Collections.EMPTY_LIST);

    }

}
