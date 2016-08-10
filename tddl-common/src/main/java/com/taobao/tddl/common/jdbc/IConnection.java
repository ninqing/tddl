package com.taobao.tddl.common.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

public interface IConnection extends Connection {

    void kill() throws SQLException;

    void cancleQuery() throws SQLException;

    long getLastInsertId();

    void setLastInsertId(long id);

}
