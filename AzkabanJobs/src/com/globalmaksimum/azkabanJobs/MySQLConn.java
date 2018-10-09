package com.globalmaksimum.azkabanJobs;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by Vace Kupecioglu on 12.01.2017.
 */
public class MySQLConn extends DBConnImpl {

    private static Properties createConnProps(String user, String pass) {
        Properties connProp = new Properties();
        connProp.put("user", user);
        connProp.put("password", pass);
        return connProp;
    }

    public MySQLConn(String user, String pass, String host, String db) {
        super(host, db, createConnProps(user, pass));
    }

    @Override
    public void open() throws SQLException {
        if (this.conn != null) {
            this.close();
        }
        conn = DriverManager.getConnection("jdbc:mysql://" + getHost() + ":3306/" + getDb(), getConnectionProps());
        conn.setAutoCommit(false);
    }
}
