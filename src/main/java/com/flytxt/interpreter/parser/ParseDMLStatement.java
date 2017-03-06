package com.flytxt.interpreter.parser;

import com.flytxt.neonstore.NeonStore;
import com.flytxt.neonstore.NeonStoreException;

/**
 * Created by aslam on 23/2/17.
 */
public class ParseDMLStatement {



    public DML parse(NeonStore store,String dml) throws ParserException,NeonStoreException {
        try{
         DML operation=   DML.valueOf(dml.substring(0,6));
         operation.evaluate(store,dml);
         return operation;
        }catch(IllegalArgumentException e) {
            throw new ParserException("Invalid Operation", e);
        }
    }
}
