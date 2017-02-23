package com.flytxt.interpreter;

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.junit.After;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by root on 22/2/17.
 */
public class NeonInterpreterTest {

    private NeonInterpreter neon;
    private InterpreterContext context;
    private InterpreterResult result;


    @org.junit.Before
    public void setUp() throws Exception {
        Properties p = new Properties();
        p.setProperty("neon.command.timeout.millisecs", "2000");
        neon = new NeonInterpreter(p);

        context = new InterpreterContext("", "1", null, "", "", null, null, null, null, null, null, null);
        neon.open();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test() {
        result = neon.interpret("INSERT INTO PROFILE ", context);

        assertEquals(InterpreterResult.Code.SUCCESS, result.code());
        assertTrue(neon.executors.isEmpty());
        // it should be fine to cancel a statement that has been completed.
        neon.cancel(context);
        assertTrue(neon.executors.isEmpty());
    }



}
