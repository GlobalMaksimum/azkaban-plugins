package com.globalmaksimum.azkabanJobs;

import azkaban.jobExecutor.AbstractJob;
import azkaban.utils.Props;
import org.apache.log4j.Logger;

/**
 * Created by Vace Kupecioglu on 10.04.2017.
 */
public class FlowRunner extends AbstractJob {

    private static final String PROJECT = "project";
    private static final String FLOW = "flow";
    private static final String FLOW_PARAMS = "flowParams";
    private final Props jobProps;
    private final Props sysProps;

    public FlowRunner(String jobId, Props sysProps, Props jobProps, Logger log) {
        super(jobId, log);
        this.jobProps = jobProps;
        this.sysProps = sysProps;
    }

    @Override
    public void run() throws Exception {
        //Validate
        validateParam(PROJECT);
        validateParam(FLOW);
        validateParam(FLOW_PARAMS);

        //Run command
        ProcessBuilder pb = new ProcessBuilder("");

        //Parse Props

        //Login

        //Execute

    }

    private void validateParam(String paramName) throws Exception {
        if (!this.jobProps.containsKey(paramName)) {
            throw new Exception(paramName + " parameter must be specified.");
        }
    }
}
