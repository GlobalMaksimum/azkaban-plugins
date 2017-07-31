package com.globalmaksimum.azkabanJobs;

import azkaban.jobExecutor.AbstractJob;
import azkaban.utils.Props;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Observable;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Vace Kupecioglu on 12.01.2017.
 */
public class TestJob extends AbstractJob {

    Props jobProps;
    Props sysProps;

    public TestJob(String jobId, Props sysProps, Props jobProps, Logger log) {
        super(jobId, log);
        this.jobProps = jobProps;
        this.sysProps = sysProps;
    }

    @Override
    public void run() throws Exception {
//        info(this.sysProps.getString("vace", "olmadi"));
//        Props vaceprops = new Props();
//        vaceprops.put("deneme", "oldu");
//        this.sysProps.setParent(vaceprops);

        //String sql = "select * from {{table_name}} where col = '{{dt}}'";
        //info(replaceParams(sql));


        String cmdOut = runCommand("python echo.py");
        System.out.println(cmdOut);
        Properties params = new Properties();
        params.load(new StringReader(cmdOut));
        for (Object key : params.keySet()) {
            System.out.println(key.toString());
        }

    }

    void executeFlow() throws IOException {
        OkHttpClient client = new OkHttpClient();
        Request req = new Request.Builder().url("http://dev:8081/executor?ajax=executeFlow" +
                "&session.id=982b4dad-ef21-4d0b-ae65-ccf9f3332ee2" +
                "&project=Test" +
                "&flow=test" +
                "&flowOverride[my_flow_param]=osman")
                .build();

        Response res = client.newCall(req).execute();

        System.out.print(res.message());
        System.out.print(res.body().string());
    }

    String runCommand(String cmd) {
        ProcessBuilder pb = new ProcessBuilder(Arrays.asList(cmd.trim().split("\\s+")));
        try {
            Process p = pb.start();
            InputStream cmdOutput = p.getInputStream();
            InputStream err = p.getErrorStream();
            int cmdRes = p.waitFor();
            if (cmdRes != 0) {
                return "Something went wrong:\n" + IOUtils.toString(err, Charset.forName("UTF-8"));
            }
            return IOUtils.toString(cmdOutput, Charset.forName("UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return "someting went wrong";

    }


    private String replaceParams(String sql) {
        Pattern r = Pattern.compile("^param\\.(\\w*)$");
        for (String key: this.jobProps.getKeySet()) {
            Matcher m = r.matcher(key);
            if (m.find()) {
                String paramName = m.group(1);
                String paramValue = this.jobProps.getString(key);
                info("Replacing param " + paramName + " with value " + paramValue);
                sql = sql.replaceAll("\\{\\{" + paramName +"}}", paramValue);
            }
        }

        return sql;
    }
}
