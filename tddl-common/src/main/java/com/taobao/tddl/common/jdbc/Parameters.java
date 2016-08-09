package com.taobao.tddl.common.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * @author yangzhu
 */
public class Parameters {

    private List<Map<Integer, ParameterContext>> batchParams = null;
    private boolean                              batch       = false;
    private Map<Integer, ParameterContext>       params      = new HashMap<Integer, ParameterContext>();

    public Parameters(){
    }

    public Parameters(Map<Integer, ParameterContext> currentParameter, boolean isBatch){
        this.params = currentParameter;
        this.batch = isBatch;
    }

    public Map<Integer, ParameterContext> getCurrentParameter() {
        return params;
    }

    public Map<Integer, ParameterContext> getFirstParameter() {
        if (!batch) {
            return params;
        }
        return batchParams.get(0);
    }

    /**
     * 返回批处理的参数，如果当前非批处理，返回单条记录
     */
    public List<Map<Integer, ParameterContext>> getBatchParameters() {
        if (isBatch()) {
            return this.batchParams;
        } else {
            return Arrays.asList(params);
        }
    }

    public void addBatch() {
        if (batchParams == null) {
            batchParams = new ArrayList();
        }

        batchParams.add(this.params);
        params = new HashMap();
        this.batch = true;
    }

    public boolean isBatch() {
        return this.batch;
    }

    public static void setParameters(PreparedStatement ps, Map<Integer, ParameterContext> parameterSettings)
                                                                                                            throws SQLException {
        ParameterMethod.setParameters(ps, parameterSettings);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
