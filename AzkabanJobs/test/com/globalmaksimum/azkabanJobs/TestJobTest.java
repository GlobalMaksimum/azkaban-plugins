package com.globalmaksimum.azkabanJobs;

import azkaban.utils.Props;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by Vace Kupecioglu on 7.04.2017.
 */
public class TestJobTest {
    Logger testLogger;
    @Before
    public void setUp() throws Exception {
        testLogger = Logger.getLogger("testLogger");
    }

    @Test
    public void testSqlParams() throws Exception {
        Props sysProps = new Props();

        Props jobProps = new Props();

        jobProps.put("type", "runsql");
        jobProps.put("param.table_name", "extr.test_table");
        jobProps.put("param.dt", "20170405");

        TestJob job = new TestJob("my job", sysProps, jobProps, testLogger);
        job.run();
    }
}
