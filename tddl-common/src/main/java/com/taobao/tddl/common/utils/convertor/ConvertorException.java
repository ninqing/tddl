package com.taobao.tddl.common.utils.convertor;

import com.taobao.tddl.common.exception.TddlRuntimeException;
import com.taobao.tddl.common.exception.code.ErrorCode;

public class ConvertorException extends TddlRuntimeException {

    private static final long serialVersionUID = 3270080439323296921L;

    public ConvertorException(String... params){
        super(ErrorCode.ERR_CONVERTOR, params);
    }

    public ConvertorException(Throwable cause, String... params){
        super(ErrorCode.ERR_CONVERTOR, cause, params);
    }

}
