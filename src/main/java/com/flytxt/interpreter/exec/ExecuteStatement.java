package com.flytxt.interpreter.exec;


import com.flytxt.interpreter.parser.ParseDMLStatement;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.flytxt.neonstore.NeonStore;

import java.util.concurrent.Callable;

/**
 * Created by root on 22/2/17.
 */
public class ExecuteStatement implements Callable<Integer> {
    private static Logger logger = LoggerFactory.getLogger(ExecuteStatement.class);

    private volatile String[] dml;

    private NeonStore store;
    static ParseDMLStatement sta = new ParseDMLStatement();

    public ExecuteStatement(final String[] dml,NeonStore store){
       this.dml=dml;
       this.store=store;
    }

    public Integer call() throws Exception {
        int count=0;
    try {
        for (String command : dml) {
            logger.info(command);
            command = command.trim();
            sta.parse(store, command);
            count++;
        }
    }catch (Exception e ){
        e.printStackTrace();
        store.rollback();
        throw  e;
    }
        return count;
    }
}
