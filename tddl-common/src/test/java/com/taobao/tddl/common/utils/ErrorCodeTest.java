package com.taobao.tddl.common.utils;

import org.junit.Test;

import com.taobao.tddl.common.exception.code.ErrorCode;

public class ErrorCodeTest {

    @Test
    public void testOutput() {
        System.out.println(ErrorCode.TDDL_0001.getMessage("hello"));
    }
}
