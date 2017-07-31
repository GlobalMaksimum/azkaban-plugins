package com.globalmaksimum.azkabanJobs;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by Vace Kupecioglu on 12.01.2017.
 */
public class MySQLConn extends DBConnImpl {

    public MySQLConn(String user, String pass, String host, String db) {
        super(user, pass, host, db);
    }

    @Override
    public void open() throws SQLException {
        if (this.conn != null) {
            this.close();
        }

        Properties connProp = new Properties();
        connProp.put("user", getUser());
        connProp.put("password", getPass());
        conn = DriverManager.getConnection("jdbc:mysql://" + getHost() + ":3306/" + getDb(), connProp);
        conn.setAutoCommit(false);
    }
}
