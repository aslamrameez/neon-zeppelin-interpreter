package com.flytxt.interpreter.parser;

/**
 * Created by aslam on 23/2/17.
 */
public class ParserException extends RuntimeException{

    public ParserException(String var1, Throwable var2) {
        super(var1, var2);
    }
    public ParserException(String var1){
        super(var1);
    }

}
