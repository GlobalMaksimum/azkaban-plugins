package com.globalmaksimum.azkabanJobs;

import azkaban.utils.Props;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.*;

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
        when(conn.<String>getList("select column_name from columns where table_schema || '.' || table_name = ?", "extr.test_table")).thenReturn(columns);

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
        when(conn.<String>getList("select column_name from columns where table_schema || '.' || table_name = ?", "extr.test_table")).thenReturn(columns);
        when(conn.<String>getFirst(CopyJob.COLUMN_TYPE_SQL, "extr.test_table", "AA")).thenReturn("int");
        when(conn.<Long>getFirst("select max(AA) from extr.test_table")).thenReturn(new Long(11));

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
        when(conn.<String>getList("select column_name from columns where table_schema || '.' || table_name = ?", "extr.test_table")).thenReturn(columns);
        when(conn.<String>getFirst(CopyJob.COLUMN_TYPE_SQL, "extr.test_table", "AA")).thenReturn("date");
        java.sql.Date maxVal = java.sql.Date.valueOf("2016-01-01");
        when(conn.<java.sql.Date>getFirst("select max(AA) from extr.test_table")).thenReturn(maxVal);

        CopyJob job = new CopyJob(jobId, sysProps, jobProps, testLogger);
        job.runWithConn(conn);

        verify(conn, never()).runSql("TRUNCATE TABLE extr.test_table;");
        verify(conn).runSql("COPY extr.test_table (AA, BB) WITH SOURCE JDBCSource() PARSER JDBCLoader(connect='extr', query='SELECT AA, BB FROM test_table WHERE " +
                "AA > ''2016-01-01''') DIRECT ENFORCELENGTH REJECTMAX 1 STREAM NAME 'extr.test_table';");
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
        when(conn.<String>getList("select column_name from columns where table_schema || '.' || table_name = ?", "extr.test_table")).thenReturn(columns);
        when(conn.<String>getFirst(CopyJob.COLUMN_TYPE_SQL, "extr.test_table", "AA")).thenReturn("int");
        when(conn.<Integer>getFirst("select max(AA) from extr.test_table")).thenReturn(null);

        CopyJob job = new CopyJob(jobId, sysProps, jobProps, testLogger);
        job.runWithConn(conn);

        verify(conn, never()).runSql("TRUNCATE TABLE extr.test_table;");
        verify(conn).runSql("COPY extr.test_table (AA, BB) WITH SOURCE JDBCSource() PARSER JDBCLoader(connect='extr', query='SELECT AA, BB " +
                "FROM test_table') DIRECT ENFORCELENGTH REJECTMAX 1 STREAM NAME 'extr.test_table';");
        verify(conn).close();
    }

    @Test
    public void mappedColumns() throws Exception {
        String jobId = "test job";
        String table = "extr.test_table";

        Props sysProps = new Props();

        Props jobProps = new Props();
        jobProps.put("target_table", table);
        jobProps.put("source_table", "test_table");
        jobProps.put("source", "extr");
        jobProps.put("renamecolumn.AA", "CC");

        DBConn conn = mock(DBConn.class);
        List<String> columns = new ArrayList<String>();
        columns.add("AA");
        columns.add("BB");
        when(conn.<String>getList("select column_name from columns where table_schema || '.' || table_name = ?", "extr.test_table")).thenReturn(columns);

        CopyJob job = new CopyJob(jobId, sysProps, jobProps, testLogger);
        job.runWithConn(conn);

        verify(conn).runSql("TRUNCATE TABLE extr.test_table;");
        verify(conn).runSql("COPY extr.test_table (AA, BB) WITH SOURCE JDBCSource() PARSER JDBCLoader(connect='extr', query='SELECT CC, BB FROM test_table') DIRECT ENFORCELENGTH REJECTMAX 1 STREAM NAME 'extr.test_table';");
        verify(conn).close();
    }

    @Test
    public void mappedMultipleColumns() throws Exception {
        String jobId = "test job";
        String table = "extr.test_table";

        Props sysProps = new Props();

        Props jobProps = new Props();
        jobProps.put("target_table", table);
        jobProps.put("source_table", "test_table");
        jobProps.put("source", "extr");
        jobProps.put("renamecolumn.AA", "EE");
        jobProps.put("renamecolumn.CC", "FF");

        DBConn conn = mock(DBConn.class);
        List<String> columns = new ArrayList<String>();
        columns.add("AA");
        columns.add("BB");
        columns.add("CC");
        columns.add("DD");
        when(conn.<String>getList("select column_name from columns where table_schema || '.' || table_name = ?", "extr.test_table")).thenReturn(columns);

        CopyJob job = new CopyJob(jobId, sysProps, jobProps, testLogger);
        job.runWithConn(conn);

        verify(conn).runSql("TRUNCATE TABLE extr.test_table;");
        verify(conn).runSql("COPY extr.test_table (AA, BB, CC, DD) WITH SOURCE JDBCSource() PARSER JDBCLoader(connect='extr', query='SELECT EE, BB, FF, DD FROM test_table') DIRECT ENFORCELENGTH REJECTMAX 1 STREAM NAME 'extr.test_table';");
        verify(conn).close();
    }


    @Test
    public void mappedColumnsWhenIncremetal() throws Exception {
        String jobId = "test job";
        String table = "extr.test_table";

        Props sysProps = new Props();

        Props jobProps = new Props();
        jobProps.put("target_table", table);
        jobProps.put("source_table", "test_table");
        jobProps.put("source", "extr");
        jobProps.put("renamecolumn.AA", "CC");
        jobProps.put("increment_by", "BB");

        DBConn conn = mock(DBConn.class);
        List<String> columns = new ArrayList<String>();
        columns.add("AA");
        columns.add("BB");
        when(conn.<String>getList("select column_name from columns where table_schema || '.' || table_name = ?", "extr.test_table")).thenReturn(columns);
        when(conn.<String>getFirst(CopyJob.COLUMN_TYPE_SQL, "extr.test_table", "BB")).thenReturn("int");
        when(conn.<Long>getFirst("select max(BB) from extr.test_table")).thenReturn(new Long(11));

        CopyJob job = new CopyJob(jobId, sysProps, jobProps, testLogger);
        job.runWithConn(conn);

        verify(conn, never()).runSql("TRUNCATE TABLE extr.test_table;");
        verify(conn).runSql("COPY extr.test_table (AA, BB) WITH SOURCE JDBCSource() PARSER JDBCLoader(connect='extr', query='SELECT CC, BB FROM test_table WHERE " +
                "BB > 11') DIRECT ENFORCELENGTH REJECTMAX 1 STREAM NAME 'extr.test_table';");
        verify(conn).close();
    }


    @Test
    public void mappedColumnsWithMappedIncremetKey() throws Exception {
        String jobId = "test job";
        String table = "extr.test_table";

        Props sysProps = new Props();

        Props jobProps = new Props();
        jobProps.put("target_table", table);
        jobProps.put("source_table", "test_table");
        jobProps.put("source", "extr");
        jobProps.put("renamecolumn.AA", "CC");
        jobProps.put("increment_by", "AA");

        DBConn conn = mock(DBConn.class);
        List<String> columns = new ArrayList<String>();
        columns.add("AA");
        columns.add("BB");
        when(conn.<String>getList("select column_name from columns where table_schema || '.' || table_name = ?", "extr.test_table")).thenReturn(columns);
        when(conn.<String>getFirst(CopyJob.COLUMN_TYPE_SQL, "extr.test_table", "AA")).thenReturn("int");
        when(conn.<Long>getFirst("select max(AA) from extr.test_table")).thenReturn(new Long(11));

        CopyJob job = new CopyJob(jobId, sysProps, jobProps, testLogger);
        job.runWithConn(conn);

        verify(conn, never()).runSql("TRUNCATE TABLE extr.test_table;");
        verify(conn).runSql("COPY extr.test_table (AA, BB) WITH SOURCE JDBCSource() PARSER JDBCLoader(connect='extr', query='SELECT CC, BB FROM test_table WHERE " +
                "CC > 11') DIRECT ENFORCELENGTH REJECTMAX 1 STREAM NAME 'extr.test_table';");
        verify(conn).close();
    }

    @Test
    public void mapColumnNames() {

        Props jobProps = new Props();
        jobProps.put("renamecolumn.AA", "CC");

        List<String> cols = Arrays.asList(new String[]{"AA", "BB"});

        CopyJob job = new CopyJob("", null, jobProps, null);
        List<String> ret = job.mapColumnNames(cols);

        assertEquals(ret.get(0), "CC");
        assertEquals(ret.get(1), "BB");

    }


}