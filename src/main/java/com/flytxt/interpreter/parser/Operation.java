package com.flytxt.interpreter.parser;

import com.flytxt.neonstore.NeonStore;

/**
 * Created by aslam on 23/2/17.
 */
public abstract class Operation {


    public abstract void  executeDML(NeonStore store, String dml);
}
