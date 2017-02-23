package com.flytxt.interpreter;

import com.flytxt.interpreter.exec.ExecuteStatement;

import com.flytxt.neonstore.NeonStoreException;
import com.flytxt.neonstore.NeonStoreInit;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.*;

import com.flytxt.neonstore.NeonStore;
import static org.apache.zeppelin.interpreter.Interpreter.*;
import static org.apache.zeppelin.interpreter.InterpreterResult.Code;


/**
 * Created by aslam on 21/2/17.
 */
public class NeonInterpreter extends Interpreter {

    private static final String TIMEOUT_PROPERTY = "neon.command.timeout.millisecs";

    private static final String MARATHON_URL ="marathon.url";
    private static final String DATABASE_USER ="database.user";
    private static final String DATABASE_PASSWORD ="database.password";
    private static final String DATABASE_SCHEMA ="database.schema";

    public static Logger logger = LoggerFactory.getLogger(NeonInterpreter.class);
    ConcurrentHashMap<String, Future<Integer>> executors;

    ExecutorService service;
    String marathonurl;
    String databaseuser;
    String databasepassword;
    String schema;


    public NeonInterpreter(Properties property) {
        super(property);
    }
    public void open() {
        logger.info("Command timeout property: {}", getProperty(TIMEOUT_PROPERTY));
        logger.info("Marathon url: {}", getProperty(MARATHON_URL));
        logger.info("Database user: {}", getProperty(DATABASE_USER));
        logger.debug("Database password: {}", getProperty(DATABASE_PASSWORD));
        logger.info("Database schema: {}", getProperty(DATABASE_SCHEMA));
        marathonurl=getProperty(MARATHON_URL);
        databaseuser=getProperty(DATABASE_USER);
        databasepassword=getProperty(DATABASE_PASSWORD);
        schema=getProperty(DATABASE_SCHEMA);
        executors = new ConcurrentHashMap<String, Future<Integer>>();
        service = Executors.newCachedThreadPool();
    }

    public void close() {

    }

    public InterpreterResult interpret(String s, InterpreterContext contextInterpreter) {
        logger.debug("Run neon dml '" + s + "'");
        Future<Integer> result=null;
        NeonStore store=null;
        try {
            String[] lines = StringUtils.split(s, '\n');
            String cmd = StringUtils.join(lines, "");

            String[] dmlstatements = StringUtils.split(cmd, ";");
            logger.info("Paragraph " + contextInterpreter.getParagraphId()
                    + " return with exit value: " + 0);
            store= NeonStoreInit.builder().
                    marathonurl(marathonurl)
                    .username(databaseuser)
                    .password(databasepassword)
                    .schema(schema)
                    .build();
            result =service.submit(new ExecuteStatement(dmlstatements,store));
            executors.put(contextInterpreter.getParagraphId(),  result);

            logger.info("Paragraph " + contextInterpreter.getParagraphId()
                    + " return with exit value: " + 0);

            result.get(Long.parseLong(getProperty(TIMEOUT_PROPERTY)),TimeUnit.MILLISECONDS);
            return new InterpreterResult(Code.SUCCESS, "SUCCESS");

        } catch (TimeoutException e) {
            if(store !=null)
                try {
                    store.rollback();
                } catch (NeonStoreException e1) {
                    e1.printStackTrace();
                }
            int exitValue = -1;
            logger.error("Can not run " + s, e);
            Code code = Code.ERROR;
            String message = "";
            if (exitValue == 143) {
                code = Code.INCOMPLETE;
                message += "Paragraph received a SIGTERM\n";
                logger.info("The paragraph " + contextInterpreter.getParagraphId()
                        + " stopped executing: " + message);
            }
            message += "ExitValue: " + exitValue;
            result.cancel(true);

            return new InterpreterResult(code, message);
        }catch(Exception e){
            if(store !=null)
                try {
                    store.rollback();
                } catch (NeonStoreException e1) {
                    e1.printStackTrace();
                }
            return new InterpreterResult(Code.ERROR, e.getMessage());
        }

        finally {
            try {
                store.commit();
            } catch (NeonStoreException e) {
                e.printStackTrace();
            }
            executors.remove(contextInterpreter.getParagraphId()).cancel(true);
        }
    }

    public void cancel(InterpreterContext context) {
        Future<Integer> executor = executors.remove(context.getParagraphId());
        if (executor != null) {
            executor.cancel(true);
        }
    }

    public FormType getFormType() {
        return FormType.SIMPLE;
    }

    public int getProgress(InterpreterContext interpreterContext) {
        return 0;
    }
}
