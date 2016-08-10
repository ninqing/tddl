package com.taobao.tddl.common.exception;

import java.sql.SQLException;

import com.taobao.tddl.common.exception.code.ErrorCode;

/**
 * Tddl nestabled {@link Exception}
 * 
 * @author jianghang 2013-10-24 下午2:55:38
 * @since 5.0.0
 */
public class TddlException extends SQLException {

    private static final long serialVersionUID = 1540164086674285095L;

    public TddlException(ErrorCode errorCode, String... params){
        super(errorCode.getMessage(params), "ERROR", errorCode.getCode());
    }

    public TddlException(ErrorCode errorCode, Throwable cause, String... params){
        super(errorCode.getMessage(params), "ERROR", errorCode.getCode(), cause);
    }

    public TddlException(Throwable cause){
        super(cause);
    }

    public String toString() {
        return getLocalizedMessage();
    }
}
