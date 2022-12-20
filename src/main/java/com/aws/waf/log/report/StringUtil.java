package com.aws.waf.log.report;

public class StringUtil {

    public static String leftFill(String data, String flag, int length){

        for (int i=data.length(); i<length; i++){
            data = flag + data;
        }
        return data;
    }

}
