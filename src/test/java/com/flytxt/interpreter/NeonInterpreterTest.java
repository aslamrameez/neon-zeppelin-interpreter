package com.flytxt.interpreter;

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.resource.LocalResourcePool;
import org.apache.zeppelin.resource.ResourcePool;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by root on 22/2/17.
 */
@Ignore
public class NeonInterpreterTest {

    private NeonInterpreter neon;
    private InterpreterContext context;
    private InterpreterResult result;


    @org.junit.Before
    public void setUp() throws Exception {
        Properties p = new Properties();
        p.setProperty("marathon.url","http://192.168.150.45:8080");
        p.setProperty("database.schema","neon");
        p.setProperty("database.user","root");
        p.setProperty("database.password","bullet");
        p.setProperty("file.path","/Files/Global/Ostrich_Uploader/neon");
        p.setProperty("neon.command.timeout.millisecs", "2000");
        neon = new NeonInterpreter(p);
        ResourcePool pool = new LocalResourcePool("testing");
        context = new InterpreterContext("", "1", null, "", "", null, null, null, null, pool, null, null);
        neon.open();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test() {

        result = neon.interpret("INSERT INTO PROFILE VALUES('rameez',3433,565,5677),('aslam1',3433,565,5677),('asdsada',3433,565,5677)", context);

        assertEquals(InterpreterResult.Code.SUCCESS, result.code());
        assertTrue(neon.executors.isEmpty());
        // it should be fine to cancel a statement that has been completed.
        neon.cancel(context);
        assertTrue(neon.executors.isEmpty());
    }

    @Test
    public void test2() {
        result = neon.interpret("INSERT INTO PROFILE ('head',3433,565,5677) VALUES('rameez1',3433,565,5677),('aslam2',3433,565,5677),('asdsada3',3433,565,5677)", context);

        assertEquals(InterpreterResult.Code.SUCCESS, result.code());
        assertTrue(neon.executors.isEmpty());
        // it should be fine to cancel a statement that has been completed.
        neon.cancel(context);
        assertTrue(neon.executors.isEmpty());
    }

    @Test
    public void update() {

        context.getResourcePool().put("pen","1");
        context.getResourcePool().put("msisdn","919172929920");

        result = neon.interpret("UPDATE METRIC SET optype=4,metric_Value=20,day_Aggr=1,month_Aggr=1,week_Aggr=1,date=14022017 WHERE msisdn=919172929920 AND name='Usage1' AND partner_Id=z(pen)" +
                "\n", context);

        assertEquals(InterpreterResult.Code.SUCCESS, result.code());
        assertTrue(neon.executors.isEmpty());
        // it should be fine to cancel a statement that has been completed.
        neon.cancel(context);
        assertTrue(neon.executors.isEmpty());
    }



}
