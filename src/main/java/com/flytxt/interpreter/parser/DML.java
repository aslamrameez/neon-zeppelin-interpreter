package com.flytxt.interpreter.parser;

import com.flytxt.neonstore.NeonStore;
import com.flytxt.neonstore.NeonStoreException;

import java.util.regex.Pattern;

/**
 * Created by aslam on 23/2/17.
 */
public enum DML {

    INSERT("INSERT\\s+INTO\\s+(\\w+?)\\s+VALUES\\s*(\\(.+?\\))","INSERT\\s+INTO\\s+(\\w+?)\\s?(\\(.*?\\))\\s+VALUES\\s*(\\(.+?\\))"){
          void evaluate(NeonStore store,String dml)throws NeonStoreException{
            InsertStatement insert=  new InsertStatement(this.regex,dml);
            insert.executeDML(store);
        }
    },

    UPDATE("UPDATE\\s+(\\w+?)\\s+SET\\s+(.*?)\\s+WHERE\\s+(.*?)","UPDATE\\s+(\\w+?)\\s+SET\\s+(.*?)"){
         void evaluate(NeonStore store,String dml)throws NeonStoreException {
            new UpdateStatement(this.regex,dml).executeDML(store);
        }
    };

    Pattern[] regex;

    DML(String ...regexs){
     this.regex=   new Pattern[regexs.length];
     for(int i=0;i<regexs.length;i++)
        this.regex[i]=Pattern.compile(regexs[i],Pattern.CASE_INSENSITIVE);
    }


     abstract void evaluate(NeonStore store,String dml) throws NeonStoreException;
}
