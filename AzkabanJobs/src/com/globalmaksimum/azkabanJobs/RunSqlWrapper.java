package com.globalmaksimum.azkabanJobs;

import azkaban.jobExecutor.JavaProcessJob;
import azkaban.utils.Props;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.List;
import java.util.StringTokenizer;


/**
 * Created by Vace Kupecioglu on 13.10.2016.
 */
public class RunSqlWrapper extends JavaProcessJob {
    private static String DRIVER_CLASSPATH="driver.classpath";
    public RunSqlWrapper(String jobid, Props sysProps, Props jobProps, Logger logger) {
        super(jobid, sysProps, jobProps, logger);
        String prefix = "db."+jobProps.getString("db", "vertica");
        sysProps.getKeySet().stream().filter(i->i.startsWith(prefix)).forEach(filtered->{
            jobProps.put(filtered,sysProps.get(filtered));
        });
    }

    protected String getJavaClass() {
        return this.getSysProps().getString("java.class");
    }

    protected String getJVMArguments() {
        return this.getSysProps().getString("jvm.args", (String)null);
    }

    @Override
    protected List<String> getClassPaths() {
        List<String> classPaths = super.getClassPaths();
        if (this.getSysProps().containsKey(DRIVER_CLASSPATH)) {
            List<String> driverClasspath = this.getSysProps().getStringList(DRIVER_CLASSPATH);
            classPaths.addAll(driverClasspath);
        }
        classPaths.add(getSourcePathFromClass(RunSqlWrapper.class));
        classPaths.add(getSourcePathFromClass(Logger.class));
        classPaths.add(getSourcePathFromClass(Props.class));
        classPaths.add(getSourcePathFromClass(ResultSetHandler.class));
        classPaths.add(getSourcePathFromClass(StringUtils.class));
        classPaths.add(getSourcePathFromClass(com.google.common.io.Files.class));
        return classPaths;
    }

    private static String getSourcePathFromClass(Class<?> containedClass) {
        File file = new File(containedClass.getProtectionDomain().getCodeSource().getLocation().getPath());
        if (!file.isDirectory() && file.getName().endsWith(".class")) {
            String name = containedClass.getName();

            for(StringTokenizer tokenizer = new StringTokenizer(name, "."); tokenizer.hasMoreTokens(); file = file.getParentFile()) {
                tokenizer.nextElement();
            }

            return file.getPath();
        } else {
            return containedClass.getProtectionDomain().getCodeSource().getLocation().getPath();
        }
    }

}
