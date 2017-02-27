package com.flytxt.interpreter.parser;

import com.flytxt.neonstore.NeonStore;

import java.util.regex.Pattern;

/**
 * Created by aslam on 23/2/17.
 */
public enum DML {

    INSERT("INSERT\\s+INTO\\s+VALUES"){
        public  void evaluate(NeonStore store,String dml){
        new InsertStatement(this.regex).executeDML(store,dml);
        }
    },

    UPDATE("UPDATE \\w+ SET|w+\\s+ "){
        public void evaluate(NeonStore store,String dml){
            new UpdateStatement(this.regex).executeDML(store,dml);
        }
    };

    Pattern[] regex;

    DML(String ...regexs){
     this.regex=   new Pattern[regexs.length];
     for(int i=0;i<regexs.length;i++)
        this.regex[i]=Pattern.compile(regexs[i]);
    }


    public abstract void evaluate(NeonStore store,String dml);
}
