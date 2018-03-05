package com.globalmaksimum.azkabanJobs;

import azkaban.jobtype.HadoopConfigurationInjector;
import azkaban.jobtype.HadoopJobUtils;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.Props;
import org.apache.log4j.Logger;

public class AsProxyImpalaJob extends AsProxyProcessJob {
    public static final String HADOOP_OPTS = ENV_PREFIX + "HADOOP_OPTS";
    public static final String HADOOP_GLOBAL_OPTS = "hadoop.global.opts";
    public static final String IMPALA_SCRIPT = "impala.script";
    public static final String IMPALA_DRIVER = "impala.driver";
    public static final String IMPALA_BASEURL = "impala.baseurl";
    public static final String SILENT = "silent";
    private String userToProxy;
    private boolean shouldProxy = false;
    private HadoopSecurityManager hadoopSecurityManager;
    private String baseImpalaUrl;
    private String impalaDriver;

    public AsProxyImpalaJob(String jobId, Props sysProps, Props jobProps, Logger log) {
        super(jobId, sysProps, jobProps, log);
        shouldProxy = getSysProps().getBoolean(HadoopSecurityManager.ENABLE_PROXYING, false);
        if (shouldProxy) {
            getLog().info("Initiating hadoop security manager.");
            try {
                hadoopSecurityManager = HadoopJobUtils.loadHadoopSecurityManager(getSysProps(), log);
            } catch (RuntimeException e) {
                throw new RuntimeException("Failed to get hadoop security manager!" + e);
            }
        }
    }

    private String getKrb5ccname(Props jobProps) {
        String projectName = jobProps.getString("azkaban.flow.projectname").replace(" ", "_");
        String flowId = jobProps.getString("azkaban.flow.flowid").replace(" ", "_");
        String jobId = jobProps.getString("azkaban.job.id").replace(" ", "_");
        String execId = jobProps.getString("azkaban.flow.execid");
        String krb5ccname = String.format("/tmp/krb5cc__%s__%s__%s__%s", projectName, flowId, jobId, execId);
        return krb5ccname;
    }

    @Override
    public void run() throws Exception {
        setupHadoopOpts(getJobProps());
        userToProxy = getJobProps().getString("user.to.proxy");
        if (userToProxy == null || userToProxy.isEmpty()) {
            throw new IllegalArgumentException("should proxy required");
        }
        baseImpalaUrl = getJobProps().getString(IMPALA_BASEURL);
        String jdbcUrl = String.format("%s;DelegationUID=%s", baseImpalaUrl, userToProxy);

        impalaDriver = getJobProps().getString(IMPALA_DRIVER);
        String impalaScript = getJobProps().getString(IMPALA_SCRIPT);
        boolean isSilent = getJobProps().getBoolean(SILENT, false);
        getJobProps().put("env.KRB5CCNAME", getKrb5ccname(getJobProps()));

        getJobProps().put("command", String.format("kinit %s -k -t %s", sysProps.getString("proxy.user"),sysProps.getString("proxy.keytab.location")));
        String command = String.format("beeline -d \"%s\" --isolation=TRANSACTION_READ_UNCOMMITTED -u \"%s\" -f %s --silent=%s", impalaDriver, jdbcUrl, impalaScript, Boolean.toString(isSilent));
        getJobProps().put("command.1", command);
        getJobProps().put("command.2", "kdestroy");
        HadoopConfigurationInjector.prepareResourcesToInject(getJobProps(), getWorkingDirectory());
        try {
            super.run();
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e);
        }
    }

    private void setupHadoopOpts(Props props) {
        if (props.containsKey(HADOOP_GLOBAL_OPTS)) {
            String hadoopGlobalOps = props.getString(HADOOP_GLOBAL_OPTS);
            if (props.containsKey(HADOOP_OPTS)) {
                String hadoopOps = props.getString(HADOOP_OPTS);
                props.put(HADOOP_OPTS, String.format("%s %s", hadoopOps, hadoopGlobalOps));
            } else {
                props.put(HADOOP_OPTS, hadoopGlobalOps);
            }
        }
    }
}
