package com.taobao.tddl.atom.utils;

import java.io.InputStream;
import java.sql.Statement;

import com.alibaba.druid.pool.DruidPooledStatement;
import com.taobao.tddl.common.jdbc.IStatement;

public class LoadFileUtils {

    public static void setLocalInfileInputStream(Statement stmt, InputStream stream) {

        if (stmt instanceof IStatement) {
            ((IStatement) stmt).setLocalInfileInputStream(stream);
            return;
        }

        if (stmt instanceof com.mysql.jdbc.Statement) {
            ((com.mysql.jdbc.Statement) stmt).setLocalInfileInputStream(stream);
            return;
        }

        if (stmt instanceof DruidPooledStatement) {
            stmt = ((DruidPooledStatement) stmt).getStatement();

            setLocalInfileInputStream(stmt, stream);

            return;
        }

        throw new UnsupportedOperationException("setLocalInfileInputStream is not supported for class:"
                                                + stmt.getClass().getName());

    }

    public static InputStream getLocalInfileInputStream(Statement stmt) {
        if (stmt instanceof IStatement) {
            return ((IStatement) stmt).getLocalInfileInputStream();
        }

        if (stmt instanceof com.mysql.jdbc.Statement) {
            return ((com.mysql.jdbc.Statement) stmt).getLocalInfileInputStream();
        }

        if (stmt instanceof DruidPooledStatement) {
            stmt = ((DruidPooledStatement) stmt).getStatement();
            return getLocalInfileInputStream(stmt);
        }

        throw new UnsupportedOperationException("getLocalInfileInputStream is not supported for class:"
                                                + stmt.getClass().getName());
    }
}
