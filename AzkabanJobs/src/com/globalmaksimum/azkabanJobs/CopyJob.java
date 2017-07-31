package com.globalmaksimum.azkabanJobs;

import azkaban.jobExecutor.AbstractJob;
import azkaban.utils.Props;
import com.google.common.base.Joiner;
import org.apache.log4j.Logger;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * Created by Vace Kupecioglu on 24.10.2016.
 */
public class CopyJob extends AbstractJob {
    public static final String WORKING_DIR = "working.dir";
    public static final String FILE = "file";
    public static final String TARGET_TABLE = "target_table";
    public static final String SOURCE_TABLE = "source_table";
    public static final String SOURCE = "source";
    public static final String COLUMN_TYPE_SQL = "select data_type from columns where table_schema || '.' || table_name = ? and column_name = ?";
    public static final String INCREMENT_BY = "increment_by";
    public static final String SELECT_MAX_FROM = "select max(%s) from %s";
    public static final String COPY_ALL_SQL = "COPY %s (%s) WITH SOURCE JDBCSource() PARSER JDBCLoader(connect='%s', query='SELECT %s FROM %s') DIRECT ENFORCELENGTH REJECTMAX 1 STREAM NAME '%s';";
    public static final String COPY_INC_SQL = "COPY %s (%s) WITH SOURCE JDBCSource() PARSER JDBCLoader(connect='%s', query='SELECT %s FROM %s WHERE %s > %s') DIRECT ENFORCELENGTH REJECTMAX 1 STREAM NAME '%s';";
    public static final String COLUMNS_SQL = "select column_name from columns where table_schema || '.' || table_name = ?";

    Props jobProps;
    Props sysProps;

    public CopyJob(String jobId, Props sysProps, Props jobProps, Logger log) {
        super(jobId, log);
        this.jobProps = jobProps;
        this.sysProps = sysProps;
    }

    @Override
    public void run() throws Exception {
        if(!areCopyParamsSet()) {
            throw new Exception("Not all required parameters are set");
        }

        VerticaConn conn = new VerticaConn(
                sysProps.getString("db.vertica.user"),
                sysProps.getString("db.vertica.pass"),
                sysProps.getString("db.vertica.host"),
                sysProps.getString("db.vertica.db")
        );
        runWithConn(conn);
    }

    public void runWithConn(DBConn conn) throws Exception {
        try {
            conn.open();
            List<String> columns = conn.getList(COLUMNS_SQL, getTargetTable());

            if (columns == null || columns.size() == 0) {
                throw new Exception(String.format("Couldn't find column list for table %s", getTargetTable()));
            }

            if (isIncremental()) {
                String colType = conn.getFirst(COLUMN_TYPE_SQL, getTargetTable(), getIncremetBy());
                if(colType == null) {
                    throw new Exception(String.format("Count not find column %s for table %s.", getIncremetBy(), getTargetTable()));
                }
                String lastKeyStr = null;
                if("int".equals(colType)) {
                    Long lastKey = conn.getFirst(String.format(SELECT_MAX_FROM, getIncremetBy(), getTargetTable()));
                    if (lastKey != null) {
                        lastKeyStr = lastKey.toString();
                    }
                } else if ("date".equals(colType)) {
                    Date lastKey = conn.getFirst(String.format(SELECT_MAX_FROM, getIncremetBy(), getTargetTable()));
                    if (lastKey != null) {
                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                        lastKeyStr = String.format("''%s''", df.format(lastKey.getTime()));
                    }
                } else {
                    throw new Exception("Unsupported increment_by column type: " + colType);
                }

                if (lastKeyStr == null) {
                    info(String.format("Max value for %s is null. Will copy whole table.", getIncremetBy()));
                    String copyStmt = String.format(
                            COPY_ALL_SQL
                            , getTargetTable()
                            , Joiner.on(", ").join(columns)
                            , getSource()
                            , Joiner.on(", ").join(columns)  //TODO column mapping
                            , getSourceTable()
                            , getTargetTable()
                    );
                    info(copyStmt);
                    conn.runSql(copyStmt);
                } else {
                    String copyStmt = String.format(
                            COPY_INC_SQL
                            , getTargetTable()
                            , Joiner.on(", ").join(columns)
                            , getSource()
                            , Joiner.on(", ").join(columns)  //TODO column mapping
                            , getSourceTable()
                            , getIncremetBy()  //TODO column mapping
                            , lastKeyStr
                            , getTargetTable()
                    );
                    info(copyStmt);
                    conn.runSql(copyStmt);
                }
            } else {
                info(String.format("Truncating table %s", getTargetTable()));
                conn.runSql(String.format("TRUNCATE TABLE %s;", getTargetTable()));
                String copyStmt = String.format(
                        COPY_ALL_SQL
                        , getTargetTable()
                        , Joiner.on(", ").join(columns)
                        , getSource()
                        , Joiner.on(", ").join(columns)  //TODO column mapping
                        , getSourceTable()
                        , getTargetTable()
                );
                info(copyStmt);
                conn.runSql(copyStmt);
            }
        } finally {
            conn.close();
        }
    }

    String getTargetTable() {
        return this.jobProps.getString(TARGET_TABLE);
    }

    boolean areCopyParamsSet() {
        return this.jobProps.containsKey(TARGET_TABLE) &&
                this.jobProps.containsKey(SOURCE_TABLE) &&
                this.jobProps.containsKey(SOURCE);
    }

    public String getSource() {
        return this.jobProps.getString(SOURCE);
    }

    public String getSourceTable() {
        return this.jobProps.getString(SOURCE_TABLE);
    }

    public boolean isIncremental() {
        return this.jobProps.containsKey(INCREMENT_BY);
    }

    public String getIncremetBy() {
        return this.jobProps.containsKey(INCREMENT_BY) ? this.jobProps.getString(INCREMENT_BY) : null;
    }
}
