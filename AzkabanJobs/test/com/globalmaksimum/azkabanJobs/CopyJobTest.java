package com.globalmaksimum.azkabanJobs;

import azkaban.utils.Props;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by Vace Kupecioglu on 24.10.2016.
 */
public class CopyJobTest {
    Logger testLogger;
    @Before
    public void setUp() throws Exception {
        testLogger = Logger.getLogger("testLogger");
    }

    @Test
    public void simpleCopyStatement() throws Exception {
        String jobId = "test job";

        Props sysProps = new Props();

        Props jobProps = new Props();
        jobProps.put("target_table", "extr.test_table");
        jobProps.put("source_table", "test_table");
        jobProps.put("source", "extr");

        DBConn conn = mock(DBConn.class);
        List<String> columns = new ArrayList<String>();
        columns.add("AA");
        columns.add("BB");
        when(conn.<String>getList("select column_name from columns where table_name = ?", "extr.test_table")).thenReturn(columns);

        CopyJob job = new CopyJob(jobId, sysProps, jobProps, testLogger);
        job.runWithConn(conn);

        verify(conn).runSql("TRUNCATE TABLE extr.test_table;");
        verify(conn).runSql("COPY extr.test_table (AA, BB) WITH SOURCE JDBCSource() PARSER JDBCLoader(connect='extr', query='SELECT AA, BB FROM test_table') DIRECT ENFORCELENGTH REJECTMAX 1 STREAM NAME 'extr.test_table';");
        verify(conn).close();
    }


    @Test
    public void incrementalCopyStatement() throws Exception {
        String jobId = "test job";
        String table = "extr.test_table";

        Props sysProps = new Props();

        Props jobProps = new Props();
        jobProps.put("target_table", table);
        jobProps.put("source_table", "test_table");
        jobProps.put("source", "extr");
        jobProps.put("increment_by", "AA");

        DBConn conn = mock(DBConn.class);
        List<String> columns = new ArrayList<String>();
        columns.add("AA");
        columns.add("BB");
        when(conn.<String>getList("select column_name from columns where table_name = ?", "extr.test_table")).thenReturn(columns);
        when(conn.<String>getFirst(String.format(CopyJob.COLUMN_TYPE_SQL, "extr.test_table", "AA"))).thenReturn("int");
        when(conn.<Integer>getFirst("select max(AA) from extr.test_table")).thenReturn(11);

        CopyJob job = new CopyJob(jobId, sysProps, jobProps, testLogger);
        job.runWithConn(conn);

        verify(conn, never()).runSql("TRUNCATE TABLE extr.test_table;");
        verify(conn).runSql("COPY extr.test_table (AA, BB) WITH SOURCE JDBCSource() PARSER JDBCLoader(connect='extr', query='SELECT AA, BB FROM test_table WHERE " +
                "AA > 11') DIRECT ENFORCELENGTH REJECTMAX 1 STREAM NAME 'extr.test_table';");
        verify(conn).close();
    }

    @Test
    public void incrementalCopyStatementWithDate() throws Exception {
        String jobId = "test job";
        String table = "extr.test_table";

        Props sysProps = new Props();

        Props jobProps = new Props();
        jobProps.put("target_table", table);
        jobProps.put("source_table", "test_table");
        jobProps.put("source", "extr");
        jobProps.put("increment_by", "AA");

        DBConn conn = mock(DBConn.class);
        List<String> columns = new ArrayList<String>();
        columns.add("AA");
        columns.add("BB");
        when(conn.<String>getList("select column_name from columns where table_name = ?", "extr.test_table")).thenReturn(columns);
        when(conn.<String>getFirst(String.format(CopyJob.COLUMN_TYPE_SQL, "extr.test_table", "AA"))).thenReturn("date");
        java.sql.Date maxVal = java.sql.Date.valueOf("2016-01-01");
        when(conn.<java.sql.Date>getFirst("select max(AA) from extr.test_table")).thenReturn(maxVal);

        CopyJob job = new CopyJob(jobId, sysProps, jobProps, testLogger);
        job.runWithConn(conn);

        verify(conn, never()).runSql("TRUNCATE TABLE extr.test_table;");
        verify(conn).runSql("COPY extr.test_table (AA, BB) WITH SOURCE JDBCSource() PARSER JDBCLoader(connect='extr', query='SELECT AA, BB FROM test_table WHERE " +
                "AA > STR_TO_DATE(''20160101'', ''%Y%m%d'')') DIRECT ENFORCELENGTH REJECTMAX 1 STREAM NAME 'extr.test_table';");
        verify(conn).close();
    }

    @Test
    public void incrementalCopyStatementWithNull() throws Exception {
        String jobId = "test job";
        String table = "extr.test_table";

        Props sysProps = new Props();

        Props jobProps = new Props();
        jobProps.put("target_table", table);
        jobProps.put("source_table", "test_table");
        jobProps.put("source", "extr");
        jobProps.put("increment_by", "AA");

        DBConn conn = mock(DBConn.class);
        List<String> columns = new ArrayList<String>();
        columns.add("AA");
        columns.add("BB");
        when(conn.<String>getList("select column_name from columns where table_name = ?", "extr.test_table")).thenReturn(columns);
        when(conn.<String>getFirst(String.format(CopyJob.COLUMN_TYPE_SQL, "extr.test_table", "AA"))).thenReturn("int");
        when(conn.<Integer>getFirst("select max(AA) from extr.test_table")).thenReturn(null);

        CopyJob job = new CopyJob(jobId, sysProps, jobProps, testLogger);
        job.runWithConn(conn);

        verify(conn, never()).runSql("TRUNCATE TABLE extr.test_table;");
        verify(conn).runSql("COPY extr.test_table (AA, BB) WITH SOURCE JDBCSource() PARSER JDBCLoader(connect='extr', query='SELECT AA, BB " +
                "FROM test_table') DIRECT ENFORCELENGTH REJECTMAX 1 STREAM NAME 'extr.test_table';");
        verify(conn).close();
    }
}