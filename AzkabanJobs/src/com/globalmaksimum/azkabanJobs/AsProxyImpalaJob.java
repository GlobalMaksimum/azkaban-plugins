package com.globalmaksimum.azkabanJobs;

import azkaban.jobtype.HadoopConfigurationInjector;
import azkaban.utils.Props;
import org.apache.log4j.Logger;

public class AsProxyImpalaJob extends AsProxyProcessJob{
    public static final String HADOOP_OPTS = ENV_PREFIX + "HADOOP_OPTS";
    public static final String HADOOP_GLOBAL_OPTS = "hadoop.global.opts";
    public static final String IMPALA_SCRIPT = "impala.script";
    public static final String IMPALA_DRIVER = "impala.driver";
    public static final String IMPALA_BASEURL = "impala.baseurl";
    public static final String SILENT = "silent";
    private String userToProxy;

    private String baseImpalaUrl;
    private String impalaDriver;
    public AsProxyImpalaJob(String jobId, Props sysProps, Props jobProps, Logger log) {
        super(jobId, sysProps, jobProps, log);
    }


    private void showProps(Props p,String name) {
        getLog().info("going to show "+name +" props");
        p.getKeySet().stream().forEach(k->getLog().info(String.format("%s : %s",k,p.get(k))));
    }
    @Override
    public void run() throws Exception {
        setupHadoopOpts(getJobProps());
        userToProxy = getJobProps().getString("user.to.proxy");
        if(userToProxy==null || userToProxy.isEmpty()){
            throw new IllegalArgumentException("should proxy required");
        }
        baseImpalaUrl = getJobProps().getString(IMPALA_BASEURL);
        String jdbcUrl = String.format("%s;DelegationUID=%s", baseImpalaUrl,userToProxy);

        impalaDriver = getJobProps().getString(IMPALA_DRIVER);
        String impalaScript = getJobProps().getString(IMPALA_SCRIPT);
        boolean isSilent = getJobProps().getBoolean(SILENT,false);


        String command = String.format("beeline -d \"%s\" --isolation=TRANSACTION_READ_UNCOMMITTED -u \"%s\" -f %s --silent=%s",impalaDriver,jdbcUrl,impalaScript,Boolean.toString(isSilent));
        getJobProps().put("command",command);
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
