package com.globalmaksimum.azkabanJobs;

import azkaban.jobExecutor.AbstractJob;
import azkaban.utils.Props;
import com.google.common.io.Files;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Vace Kupecioglu on 13.10.2016.
 */
public class RunSqlJob extends AbstractJob {
    public static final String WORKING_DIR = "working.dir";
    public static final String FILE = "file";

    Props jobProps;
    Props sysProps;

    public RunSqlJob(String jobId, Props sysProps, Props jobProps, Logger log) {
        super(jobId, log);
        this.jobProps = jobProps;
        this.sysProps = sysProps;
    }

    @Override
    public void run() throws Exception {
        if(!getJobProps().containsKey(FILE)) {
            throw new Exception(FILE + " parameter must be specified.");
        }

        String db = getJobProps().getString("db", "vertica").trim();

        if(db.equals("")) {
            throw new Exception("You must specify a db name");
        }

        if(!areConnectionParamsSet(db)) {
            throw new Exception("Connection parameters are not set for db " + db);
        }

        File sqlFile = new File(getWorkingDirectory(), getJobProps().getString(FILE));
        if(!sqlFile.exists()) {
            throw new Exception("Could not find the input file at: " + sqlFile.getAbsolutePath());
        }

        String rawSql = Files.toString(sqlFile, Charset.forName("UTF-8"));
        String sql = replaceParams(rawSql);
        info("Query to execute: ");
        info(sql);

        DBConn conn = createConn(db, sysProps);
        conn.runSql(sql);
    }

    private String replaceParams(String sql) {
        Pattern r = Pattern.compile("^param\\.(\\w*)$");
        for (String key: this.jobProps.getKeySet()) {
            Matcher m = r.matcher(key);
            if (m.find()) {
                String paramName = m.group(1);
                String paramValue = this.jobProps.getString(key);
                info("Replacing param " + paramName + " with value " + paramValue);
                sql = sql.replaceAll("\\{\\{" + paramName +"}}", paramValue);
            }
        }
        return sql;
    }

    private boolean areConnectionParamsSet(String db) {
        return  sysProps.containsKey("db." + db + ".user") &&
                sysProps.containsKey("db." + db + ".pass") &&
                sysProps.containsKey("db." + db + ".host") &&
                sysProps.containsKey("db." + db + ".db") &&
                sysProps.containsKey("db." + db + ".type");
    }

    private DBConn createConn(String db, Props sysProps) throws Exception {
        String type = sysProps.getString("db." + db + ".type", "").trim();

        if(type.equals("mysql")) {
            return new MySQLConn(
                    sysProps.getString("db." + db + ".user"),
                    sysProps.getString("db." + db + ".pass"),
                    sysProps.getString("db." + db + ".host"),
                    sysProps.getString("db." + db + ".db")
            );
        }

        if(type.equals("vertica")) {
            return new VerticaConn(
                    sysProps.getString("db." + db + ".user"),
                    sysProps.getString("db." + db + ".pass"),
                    sysProps.getString("db." + db + ".host"),
                    sysProps.getString("db." + db + ".db"),
                    sysProps.getString("db." + db + ".backupServerNode", "")
            );
        }

        throw new Exception("Unsupported db type: " + type);
    }


    public Props getJobProps() {
        return jobProps;
    }

    public String getWorkingDirectory() {
        String workingDir = getJobProps().getString(WORKING_DIR, "");
        if (workingDir == null) {
            return "";
        }

        return workingDir;
    }

}
