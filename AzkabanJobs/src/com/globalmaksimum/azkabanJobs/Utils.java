package com.globalmaksimum.azkabanJobs;

import azkaban.jobExecutor.ProcessJob;
import azkaban.utils.Props;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {

    public static Properties loadAzkabanProps() throws IOException {
        String propsFile = System.getenv(ProcessJob.JOB_PROP_ENV);
        Properties props = new Properties();
        props.load(new BufferedReader(new FileReader(propsFile)));
        return props;
    }

    public static String replaceSQLParams(Props jobProps, String sql) {
        Pattern r = Pattern.compile("^param\\.(\\w*)$");
        for (String key : jobProps.getKeySet()) {
            Matcher m = r.matcher(key);
            if (m.find()) {
                String paramName = m.group(1);
                String paramValue = jobProps.getString(key);
                System.out.println("Replacing param " + paramName + " with value " + paramValue);
                sql = sql.replaceAll("\\{\\{" + paramName + "}}", paramValue);
            }
        }
        return sql;
    }
}
