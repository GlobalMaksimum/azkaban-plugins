package com.globalmaksimum.azkabanJobs;

import azkaban.jobtype.HadoopConfigurationInjector;
import azkaban.jobtype.HadoopJobUtils;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.Props;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
        File afterVelocity = createVelocityFile();
        String processedContent = processFile(impalaScript, afterVelocity);
        getLog().info("impala query to execute after variable substitution: "+processedContent);
        getJobProps().put("command", String.format("kinit %s -k -t %s", sysProps.getString("proxy.user"), sysProps.getString("proxy.keytab.location")));
        String command = String.format("beeline -d \"%s\" --isolation=TRANSACTION_READ_UNCOMMITTED -u \"%s\" -f %s --silent=%s", impalaDriver, jdbcUrl, afterVelocity.toPath().toAbsolutePath(), Boolean.toString(isSilent));
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

    public File createVelocityFile() {
        File directory = new File(getCwd());
        try {
            return File.createTempFile(this.getId() + "_velocity_", "_tmp", directory);
        } catch (IOException var5) {
            throw new RuntimeException("Failed to create temp property file ", var5);
        }
    }

    public String processFile(String impalaScript, File out) {
        VelocityEngine engine = new VelocityEngine();
        Map<String, String> tmp = new HashMap<>();
        getJobProps().getKeySet().forEach(key-> tmp.put(key,getJobProps().get(key)));
        VelocityContext context = new VelocityContext(tmp);
        try (FileWriterWithEncoding fileWriterWithEncoding = new FileWriterWithEncoding(out, "UTF-8", false);
             Reader reader = new InputStreamReader(new FileInputStream(getCwd() + File.separator + impalaScript), Charset.forName("UTF-8"))) {
            StringWriter writer = new StringWriter();
            engine.evaluate(context, writer, "proxyimpalajob", reader);
            String str = writer.toString();
            fileWriterWithEncoding.write(str);
            return str;
        } catch (FileNotFoundException e) {
            getLog().error("could not find file",e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
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
