package com.flytxt.interpreter.parser;

import com.flytxt.interpreter.NeonInterpreter;
import com.flytxt.neonstore.NeonStore;
import com.flytxt.neonstore.NeonStoreException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by aslam on 27/2/17.
 */
class InsertStatement extends Operation{
    Pattern[] regex;

    String dml;
    public static Logger logger = LoggerFactory.getLogger(InsertStatement.class);
    InsertStatement(Pattern[] regex,String dml){
        this.dml=dml;
        this.regex=regex;
    }

    public void  executeDML(NeonStore store) throws NeonStoreException{
        String tablename="";

        String insertValue=null;
        String[] head=null;
        for(Pattern p :regex){
            Matcher match= p.matcher(dml);
           if(validate(match)) {
               int count=  match.groupCount();
               tablename=match.group(1);
               if(count>2){
                   head= extractHead(match.group(2));
                   insertValue=match.group(3);
               }
               else{
                   insertValue=match.group(2);
               }
           break;
           }
        }
        if(insertValue == null)
            throw new ParserException("Invalid SQL Exception");
        callNeonStore(this, store,tablename,insertValue,head);
    }

     void callNeonStore(InsertStatement insertStatement, NeonStore store, String table, String proj, String[] head) throws NeonStoreException {

       String[] projections= proj.split("\\),\\(");

       for(String p:projections){
         p=  insertStatement.clean(p);
         String[] value=  insertStatement.splitWithComma(p);
           java.util.Map<String,String> projectionMap=new LinkedHashMap<String, String>(15);
         if(head==null){
             head= new String[value.length];
             for(int i=0;i<head.length;i++){
                 head[i]="KEY"+i;
             }
         }
             try{
             for(int i=0;i<head.length;i++){

                 projectionMap.put(head[i].toUpperCase(),valid(value[i]));
             }
             }catch (ArrayIndexOutOfBoundsException e){
                 throw new ParserException("Insuffisant columns",e);
             }
           logger.info("projectionMap {}", projectionMap.toString());
           store.insert(table,projectionMap);
       }

    }

    String[] extractHead(String str){
        str=  clean(str);
        return splitWithComma(str);
    }

    String clean(String str){
        str=StringUtils.deleteWhitespace(str);
        str= StringUtils.removeStart(str,"(");
        return StringUtils.removeEnd(str,")");
    }





}
