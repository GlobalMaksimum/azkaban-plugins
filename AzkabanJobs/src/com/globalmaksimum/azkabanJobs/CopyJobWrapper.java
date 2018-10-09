package com.globalmaksimum.azkabanJobs;

import azkaban.utils.Props;
import com.google.common.base.Joiner;
import org.apache.log4j.Logger;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Vace Kupecioglu on 24.10.2016.
 */
public class CopyJobWrapper {
    private static final Logger logger = Logger.getRootLogger();
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
    private String jobId;

    public CopyJobWrapper(String jobId, Props jobProps) {
        this.jobId = jobId;
        this.jobProps = jobProps;
    }

    public static void main(String[] args) throws Exception {
        CopyJobWrapper copyJobWrapper = new CopyJobWrapper("empty", new Props(null, Utils.loadAzkabanProps()));
        copyJobWrapper.run();
    }

    public void run() throws Exception {
        System.out.println("Running with jobId " + this.jobId);
        String db = jobProps.getString("db", "vertica").trim();
        if (!areCopyParamsSet()) {
            throw new Exception("Not all required parameters are set");
        }

        VerticaConn conn = new VerticaConn(
                jobProps.getString("db." + db + ".user"),
                jobProps.getString("db." + db + ".pass", ""),
                jobProps.getString("db." + db + ".host"),
                jobProps.getString("db." + db + ".db"),
                jobProps.getString("db." + db + ".backupServerNode", ""),
                jobProps.getString("db." + db + ".service")
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
                if (colType == null) {
                    throw new Exception(String.format("Count not find column %s for table %s.", getIncremetBy(), getTargetTable()));
                }
                String lastKeyStr = null;
                if ("int".equals(colType)) {
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
                    logger.info(String.format("Max value for %s is null. Will copy whole table.", getIncremetBy()));
                    String copyStmt = String.format(
                            COPY_ALL_SQL
                            , getTargetTable()
                            , Joiner.on(", ").join(columns)
                            , getSource()
                            , Joiner.on(", ").join(mapColumnNames(columns))
                            , getSourceTable()
                            , getTargetTable()
                    );
                    logger.info(copyStmt);
                    conn.runSql(copyStmt);
                } else {
                    String copyStmt = String.format(
                            COPY_INC_SQL
                            , getTargetTable()
                            , Joiner.on(", ").join(columns)
                            , getSource()
                            , Joiner.on(", ").join(mapColumnNames(columns))
                            , getSourceTable()
                            , mapColumnName(getIncremetBy())
                            , lastKeyStr
                            , getTargetTable()
                    );
                    logger.info(copyStmt);
                    conn.runSql(copyStmt);
                }
            } else {
                logger.info(String.format("Truncating table %s", getTargetTable()));
                conn.runSql(String.format("TRUNCATE TABLE %s;", getTargetTable()));
                String copyStmt = String.format(
                        COPY_ALL_SQL
                        , getTargetTable()
                        , Joiner.on(", ").join(columns)
                        , getSource()
                        , Joiner.on(", ").join(mapColumnNames(columns))
                        , getSourceTable()
                        , getTargetTable()
                );
                logger.info(copyStmt);
                conn.runSql(copyStmt);
            }
        } finally {
            conn.close();
        }
    }

    public List<String> mapColumnNames(List<String> columns) {

        List<String> ret = new ArrayList<String>();

        for (String col : columns) {
            ret.add(mapColumnName(col));
        }

        return ret;
    }

    private String mapColumnName(String col) {
        String propKey = "renamecolumn." + col;
        if (!this.jobProps.containsKey(propKey)) {
            return col;
        }

        return this.jobProps.getString(propKey);
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
