package com.globalmaksimum.azkabanJobs;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Vace Kupecioglu on 12.01.2017.
 */
public abstract class DBConnImpl implements DBConn {
    Connection conn;
    String host;
    String user;
    String pass;
    String db;
    String backupServerNode;

    @Override
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    @Override
    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }

    @Override
    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    @Override
    public String getBackupServerNode() { return backupServerNode; }

    public void setBackupServerNode(String backupServerNode) { this.backupServerNode = backupServerNode; }

    public DBConnImpl(String user, String pass, String host, String db) {
        this.setUser(user.trim());
        this.setPass(pass.trim());
        this.setHost(host.trim());
        this.setDb(db.trim());
    }

    public DBConnImpl(String user, String pass, String host, String db, String backupServerNode) {
        this.setUser(user.trim());
        this.setPass(pass.trim());
        this.setHost(host.trim());
        this.setDb(db.trim());
        this.setBackupServerNode(backupServerNode.trim());
    }

    @Override
    public void close() {
        if(conn != null) {
            DbUtils.closeQuietly(this.conn);
            conn = null;
        }
    }

    @Override
    public void runSql(String sql) throws SQLException {
        boolean closeWhenReturned = false;
        if(conn == null) {
            this.open();
            closeWhenReturned = true;
        }
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.execute(sql);
            conn.commit();
        }
        finally {
            DbUtils.closeQuietly(stmt);
            if(closeWhenReturned) {
                close();
            }
        }
    }

    @Override
    public <T> List<T> getList(String sql, Object... params) throws SQLException {
        boolean closeWhenReturned = false;
        if(conn == null) {
            this.open();
            closeWhenReturned = true;
        }

        try {
            QueryRunner qr = new QueryRunner();
            return qr.query(conn, sql, new ResultSetHandler<List<T>>() {
                @Override
                public List<T> handle(ResultSet resultSet) throws SQLException {
                    ArrayList<T> res = new ArrayList<T>();
                    while(resultSet.next()) {
                        T t = (T) resultSet.getObject(1);
                        res.add(t);
                    }
                    return res;
                }
            }, params);

        } finally {
            if(closeWhenReturned) {
                close();
            }
        }
    }

    @Override
    public <T> T getFirst(String sql, Object... params) throws SQLException {
        List<T> l = getList(sql, params);
        return l.size() > 0 ? l.get(0) : null;
    }
}
