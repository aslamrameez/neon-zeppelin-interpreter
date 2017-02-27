package com.flytxt.interpreter.parser;

/**
 * Created by aslam on 23/2/17.
 */
public class ParseDMLStatement {



    public DML parse(String dml) throws ParserException{
        try{
         DML operation=   DML.valueOf(dml.substring(0,6));
         return operation;
        }catch(IllegalArgumentException e) {
            throw new ParserException("Invalid Operation", e);
        }
    }
}
