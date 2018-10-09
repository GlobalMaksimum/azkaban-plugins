package com.globalmaksimum.azkabanJobs;

import org.apache.commons.lang3.StringUtils;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by Vace Kupecioglu on 24.10.2016.
 */
public class VerticaConn extends DBConnImpl {

    public VerticaConn(String user, String pass, String host, String db, String backupServerNode, String service) {
        super(host, db, createConnProps(user, pass, host, backupServerNode, service));
    }

    private static Properties createConnProps(String user, String pass, String host, String backupServerNode, String service) {
        Properties props = new Properties();
        props.setProperty("user", user);
        if (org.apache.commons.lang3.StringUtils.isEmpty(pass)) {
            // kerberos config
            props.setProperty("KerberosServiceName", service);
            props.setProperty("KerberosHostName", host);
            props.setProperty("JAASConfigName", "verticajdbc");
        } else {
            props.setProperty("password", pass);
        }
        if (!StringUtils.isEmpty(backupServerNode)) {
            props.put("BackupServerNode", backupServerNode);
        }
        props.put("connectionLoadBalance", "true");
        return props;
    }

    @Override
    public void open() throws SQLException {
        if (this.conn != null) {
            this.close();
        }
        conn = DriverManager.getConnection("jdbc:vertica://" + getHost() + ":5433/" + getDb(), getConnectionProps());
        conn.setAutoCommit(false);
    }
}
