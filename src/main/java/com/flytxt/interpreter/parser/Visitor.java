package com.flytxt.interpreter.parser;

import java.util.Map;

/**
 * Created by aslam on 23/2/17.
 */
public interface Visitor {

     void row(Map<String,String> row);
}
