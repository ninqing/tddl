package com.taobao.tddl.common.exception;

import org.apache.commons.lang.exception.NestableRuntimeException;

/**
 * tddl nestable exception, 为rethrow exception准备的
 * 
 * @author jianghang 2014-4-23 下午1:54:17
 * @since 5.1.0
 */
public class TddlNestableRuntimeException extends NestableRuntimeException {

    private static final long serialVersionUID = -363994535286446190L;

    public TddlNestableRuntimeException(){
        super();
    }

    public TddlNestableRuntimeException(String msg, Throwable cause){
        super(msg, cause);
    }

    public TddlNestableRuntimeException(String msg){
        super(msg);
    }

    public TddlNestableRuntimeException(Throwable cause){
        super(cause);
    }

}
