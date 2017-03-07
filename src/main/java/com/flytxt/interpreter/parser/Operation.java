package com.flytxt.interpreter.parser;

import com.flytxt.neonstore.NeonStore;
import com.flytxt.neonstore.NeonStoreException;
import org.apache.commons.lang.StringUtils;
import org.jsoup.helper.StringUtil;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by aslam on 23/2/17.
 */
public abstract class Operation {


    public abstract void  executeDML(NeonStore store) throws NeonStoreException;


    protected boolean validate(Matcher match){
        return match.matches();
    }

    protected String[] splitWithComma(String str){
        return str.split("(?<!\\\\),");
    }

    protected String valid(String literal){
        if(StringUtils.isNotBlank(literal)) {
            literal = literal.trim();
            if (literal.startsWith("\'") && literal.endsWith("\'")) {
                literal = StringUtils.removeStart(literal, "\'");
                literal = StringUtils.removeEnd(literal, "\'");
                return literal;
            } else if (StringUtil.isNumeric(literal)) {
                return literal;
            }
        }
      throw new ParserException("Invalid character found "+literal);
    }
}
