package com.globalmaksimum.azkabanJobs;

import azkaban.jobtype.HadoopConfigurationInjector;
import azkaban.jobtype.HadoopSecureWrapperUtils;
import azkaban.utils.Props;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.beeline.BeeLine;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import static org.apache.hadoop.security.UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION;

public class HadoopSecureBeelineWrapper {
    private static final Logger logger = Logger.getRootLogger();
    private static String hiveScript;
    public static void main(final String[] args) throws IOException, InterruptedException {
        Properties jobProps = HadoopSecureWrapperUtils.loadAzkabanProps();
        HadoopConfigurationInjector.injectResources(new Props(null, jobProps));

        hiveScript = jobProps.getProperty("hive.script");

        if(HadoopSecureWrapperUtils.shouldProxy(jobProps)) {
            String tokenFile = System.getenv(HADOOP_TOKEN_FILE_LOCATION);
            System.setProperty("mapreduce.job.credentials.binary", tokenFile);
            UserGroupInformation proxyUser =
                    HadoopSecureWrapperUtils.setupProxyUser(jobProps, tokenFile, logger);
            proxyUser.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    runBeeline(args);
                    return null;
                }
            });
        } else {
            runBeeline(args);
        }
    }

    private static void runBeeline(String[] args) throws IOException {
        BeeLine beeline = new BeeLine();
        int status = beeline.begin(args,null);
        beeline.close();
        if(status!=0)
            System.exit(status);
    }
}
