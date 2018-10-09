package com.globalmaksimum.azkabanJobs;

import azkaban.jobtype.HadoopConfigurationInjector;
import azkaban.jobtype.HadoopShell;
import azkaban.utils.Props;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.log4j.Logger;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import java.io.*;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class UnSecureImpalaJob extends HadoopShell {
    public static final String IMPALA_SCRIPT = "impala.script";
    public static final String IMPALA_DRIVER = "impala.driver";
    public static final String IMPALA_BASEURL = "impala.baseurl";
    public static final String IMPALA_SHELL = "impala.shell";
    public static final String SILENT = "silent";
    private String baseImpalaUrl;
    private String impalaDriver;
    private String impalaShell;

    public UnSecureImpalaJob(String jobid, Props sysProps, Props jobProps, Logger log) throws RuntimeException {
        super(jobid, sysProps, jobProps, log);
    }

    @Override
    public void run() throws Exception {
        impalaShell = getSysProps().getString(IMPALA_SHELL,"impala-shell");
        baseImpalaUrl = getJobProps().getString(IMPALA_BASEURL);
        impalaDriver = getJobProps().getString(IMPALA_DRIVER);
        String impalaScript = getJobProps().getString(IMPALA_SCRIPT);
        boolean isSilent = getJobProps().getBoolean(SILENT, false);
        File afterVelocity = createVelocityFile();
        String processedContent = processFile(impalaScript, afterVelocity);
        if(this.getJobProps().containsKey("user.to.proxy"))
            getJobProps().put("env.PYTHON_EGG_CACHE","/tmp/impala-shell-python-egg-cache-"+this.getJobProps().get("user.to.proxy"));
        getLog().info("impala query to execute after variable substitution: "+processedContent);
        String command = String.format("%s -i %s -f %s %s",impalaShell,baseImpalaUrl,afterVelocity.toPath().toAbsolutePath(),isSilent?"--quiet":"-V");
        getJobProps().put("command", command);
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

}
