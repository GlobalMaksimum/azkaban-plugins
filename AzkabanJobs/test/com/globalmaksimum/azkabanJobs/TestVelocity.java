package com.globalmaksimum.azkabanJobs;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.junit.Test;

import java.io.*;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class TestVelocity {

    @Test
    public void testVelocity() {
        VelocityEngine e = new VelocityEngine();
        Map<String,String> vars = new HashMap<>();
        vars.put("table","peopleparquet");
        VelocityContext context = new VelocityContext(vars);

        StringReader reader = new StringReader("select count(*) from ${table};");
        StringWriter writer = new StringWriter();
        boolean evalResult = e.evaluate(context, writer, "log", reader);
        System.out.println(evalResult);
        System.out.println(writer.toString());
    }

    @Test
    public void testVelocityFile() throws FileNotFoundException {
        VelocityEngine e = new VelocityEngine();
        Map<String,String> vars = new HashMap<>();
        vars.put("table","peopleparquet");
        VelocityContext context = new VelocityContext(vars);
        Reader reader = new InputStreamReader(new FileInputStream("testresources/countpeople.hql"), Charset.forName("UTF-8"));
        StringWriter writer = new StringWriter();
        boolean evalResult = e.evaluate(context, writer, "log", reader);
        System.out.println(evalResult);
        System.out.println(writer.toString());
    }
}
