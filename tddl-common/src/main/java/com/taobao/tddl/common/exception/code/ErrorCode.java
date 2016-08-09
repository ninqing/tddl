package com.taobao.tddl.common.exception.code;

/**
 * 返回值定义的code对象定义
 * 
 * @author jianghang 2014-3-20 下午6:02:00
 * @since 5.0.4
 */
public enum ErrorCode {

    TDDL_0001;

    public String getMessage(String... params) {
        return ResourceBundleUtil.getInstance().getMessage(this.name(), params);
    }
}
