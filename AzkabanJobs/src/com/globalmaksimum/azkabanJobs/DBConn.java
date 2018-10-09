package com.globalmaksimum.azkabanJobs;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

/**
 * Created by Vace Kupecioglu on 24.10.2016.
 */
public interface DBConn {
    String getHost();

    String getDb();

    Properties getConnectionProps();

    void open() throws SQLException;

    void close();

    void runSql(String sql) throws SQLException;

    <T> List<T> getList(String sql, Object... params) throws SQLException;

    <T> T getFirst(String sql, Object... params) throws SQLException;
}
