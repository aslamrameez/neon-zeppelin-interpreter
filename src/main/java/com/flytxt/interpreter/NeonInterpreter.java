package com.flytxt.interpreter;

import com.flytxt.interpreter.exec.ExecuteStatement;

import com.flytxt.neonstore.NeonConfig;
import com.flytxt.neonstore.NeonStoreException;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.resource.ResourcePool;
import org.jsoup.helper.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.flytxt.neonstore.NeonStore;
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
    private static final String NEON_TMP_FILE_NAME ="file.path";
    public static Logger logger = LoggerFactory.getLogger(NeonInterpreter.class);
    ConcurrentHashMap<String, Future<Integer>> executors;

    ExecutorService service;

    NeonConfig conig;
    Pattern pattern=Pattern.compile("(z\\((.*?)\\))");


    public NeonInterpreter(Properties property) {
        super(property);
    }
    public void open() {
        logger.info("Command timeout property: {}", getProperty(TIMEOUT_PROPERTY));
        logger.info("Marathon url: {}", getProperty(MARATHON_URL));
        logger.info("Database user: {}", getProperty(DATABASE_USER));
        logger.debug("Database password: {}", getProperty(DATABASE_PASSWORD));
        logger.info("Database schema: {}", getProperty(DATABASE_SCHEMA));
        logger.info("File name : {}", getProperty(NEON_TMP_FILE_NAME));

        executors = new ConcurrentHashMap<String, Future<Integer>>();
        service = Executors.newCachedThreadPool();
        conig= NeonConfig.builder().marathonurl(getProperty(MARATHON_URL))
                .username(getProperty(DATABASE_USER))
                .password(getProperty(DATABASE_PASSWORD))
                .schema(getProperty(DATABASE_SCHEMA))
                .fileName(getProperty(NEON_TMP_FILE_NAME))
                .build();
    }

    public void close() {

    }

    public InterpreterResult interpret(String s, InterpreterContext contextInterpreter) {
        logger.debug("Run neon dml '" + s + "'");
        Future<Integer> result=null;
        NeonStore store=null;
        try {

            s=replaceFromZepplinContext(s,contextInterpreter.getResourcePool());
            String[] lines = StringUtils.split(s, '\n');
            String cmd = StringUtils.join(lines, "");

            String[] dmlstatements = StringUtils.split(cmd, ";");
            logger.info("Paragraph {}",  contextInterpreter.getParagraphId());
            store= conig.getNewNeonStore();
            result =service.submit(new ExecuteStatement(dmlstatements,store));
            executors.put(contextInterpreter.getParagraphId(),  result);

            logger.info("dml {}" , s);

            result.get(Long.parseLong(getProperty(TIMEOUT_PROPERTY)),TimeUnit.MILLISECONDS);
            store.commit();
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
               logger.error("Error in neon" ,e);

            return new InterpreterResult(Code.ERROR, e.getLocalizedMessage());
        }

        finally {

            result =  executors.remove(contextInterpreter.getParagraphId());
            if(result !=null)
                result.cancel(true);

        }
    }

   private String replaceFromZepplinContext(String s, ResourcePool pool){
        Matcher matcher= pattern.matcher(s);
        StringBuilder str= new StringBuilder(s);
        int buffer=0;
        while(matcher.find()){
            int groupOne=matcher.group(1).length();
            String string= matcher.group(2).trim();
            if(StringUtils.startsWith(string,"\"") && StringUtils.endsWith(string,"\"")){
                string=  StringUtils.removeStart(string,"\"");
                string=StringUtils.removeEnd(string,"\"");
            logger.debug("replace string {}",string);
            String replace_str=pool.get(string).get().toString();
            str.replace(buffer+matcher.start(1),buffer+matcher.end(1), replace_str);
            buffer +=replace_str.length()-groupOne;
            }
        }
        return str.toString();
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
