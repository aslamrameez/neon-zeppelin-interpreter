package com.flytxt.interpreter.parser;

import com.flytxt.neonstore.NeonStore;
import com.flytxt.neonstore.NeonStoreException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by aslam on 27/2/17.
 */
public class UpdateStatement extends Operation{
    Pattern[] regex;

    String dml;
    public static Logger logger = LoggerFactory.getLogger(UpdateStatement.class);
    UpdateStatement(Pattern[] regex,String dml){
        this.dml=dml;
        this.regex=regex;
    }

    public void  executeDML(NeonStore store)throws NeonStoreException {
        String tablename="";
        Map<String,String> conditionMap=null;
        Map<String,String> updateValue=null;
        for(Pattern p :regex){
            Matcher match= p.matcher(dml);
            if(validate(match)) {
                tablename=match.group(1);
                int count=match.groupCount();
                 updateValue=createProjectionMap(match.group(2));

                if(count>2) {
                    conditionMap=    createConditionMap(match.group(3));
                    logger.info("condition {}", conditionMap.toString());
                }
                break;
            }
        }
        logger.info("updateValue {}", updateValue.toString());
        store.update(tablename,updateValue,conditionMap);
    }

  Map<String,String>  createProjectionMap(String projection){
      String[] proj=splitWithComma(projection);
     Map<String,String> map = new HashMap<String, String>();
      for(String p:proj){
          String[] s=p.split("=");
          map.put(s[0].trim().toUpperCase(),valid(s[1]));
      }
      return map;
    }
    Map<String,String>  createConditionMap(String projection){
        String[] proj=projection.split("(?i)(and|or)");
        Map<String,String> map = new HashMap<String, String>();
        for(String p:proj){
            String[] s=p.split("=");
            map.put(s[0].trim().toUpperCase(),valid(s[1]));
        }
        return map;
    }
}
