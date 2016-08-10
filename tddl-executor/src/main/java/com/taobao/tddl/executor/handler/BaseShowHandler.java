package com.taobao.tddl.executor.handler;

import com.taobao.tddl.common.exception.TddlException;
import com.taobao.tddl.executor.common.ExecutionContext;
import com.taobao.tddl.executor.cursor.ISchematicCursor;
import com.taobao.tddl.optimizer.core.plan.IDataNodeExecutor;

public abstract class BaseShowHandler extends HandlerCommon {

    @Override
    public ISchematicCursor handle(IDataNodeExecutor executor, ExecutionContext executionContext) throws TddlException {
        // TODO Auto-generated method stub
        return null;
    }

    public abstract ISchematicCursor doShow(IDataNodeExecutor executor, ExecutionContext executionContext)
                                                                                                          throws TddlException;

}
