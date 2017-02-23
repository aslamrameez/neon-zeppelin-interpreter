package com.flytxt.interpreter.parser;

/**
 * Created by aslam on 23/2/17.
 */
public enum DML implements Operation{
    INSERT("INSERT\\s+INTO\\s+VALUES|values"),UPDATE,DELETE;
}
