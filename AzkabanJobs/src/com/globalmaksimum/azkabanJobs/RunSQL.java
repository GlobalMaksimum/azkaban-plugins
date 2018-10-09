package com.globalmaksimum.azkabanJobs;

import azkaban.utils.Props;
import com.google.common.io.Files;

import java.io.File;
import java.nio.charset.Charset;

import static com.globalmaksimum.azkabanJobs.Utils.replaceSQLParams;

public class RunSQL {
    private static final String FILE = "file";

    public static void main(String[] args) throws Exception {

        Props jobProps = new Props(null, Utils.loadAzkabanProps());
        if (!jobProps.containsKey(FILE)) {
            throw new Exception(FILE + " parameter must be specified.");
        }

        String db = jobProps.getString("db", "vertica").trim();

        if (db.equals("")) {
            throw new Exception("You must specify a db name");
        }

        if (!areConnectionParamsSet(jobProps, db)) {
            jobProps.getKeySet().forEach(key -> {
                System.out.println(key + "=" + jobProps.get(key));
            });
            throw new Exception("Connection parameters are not set for db " + db);
        }

        File sqlFile = new File(jobProps.getString(FILE));
        if (!sqlFile.exists()) {
            throw new Exception("Could not find the input file at: " + sqlFile.getAbsolutePath());
        }

        String rawSql = Files.toString(sqlFile, Charset.forName("UTF-8"));
        String sql = replaceSQLParams(jobProps, rawSql);
        System.out.println("Query to execute: ");
        System.out.println(sql);

        DBConn conn = createConn(db, jobProps);
        conn.runSql(sql);
    }

    private static boolean areConnectionParamsSet(Props jobProps, String db) {
        return jobProps.containsKey("db." + db + ".host") &&
                jobProps.containsKey("db." + db + ".db") &&
                jobProps.containsKey("db." + db + ".type");
    }

    private static DBConn createConn(String db, Props sysProps) throws Exception {
        String type = sysProps.getString("db." + db + ".type", "").trim();

        if (type.equals("mysql")) {
            return new MySQLConn(
                    sysProps.getString("db." + db + ".user"),
                    sysProps.getString("db." + db + ".pass"),
                    sysProps.getString("db." + db + ".host"),
                    sysProps.getString("db." + db + ".db")
            );
        }

        if (type.equals("vertica")) {
            return new VerticaConn(
                    sysProps.getString("db." + db + ".user"),
                    sysProps.getString("db." + db + ".pass", null),
                    sysProps.getString("db." + db + ".host"),
                    sysProps.getString("db." + db + ".db", "vertica"),
                    sysProps.getString("db." + db + ".backupServerNode", ""),
                    sysProps.getString("db." + db + ".service",null)
            );
        }

        throw new Exception("Unsupported db type: " + type);
    }
}
