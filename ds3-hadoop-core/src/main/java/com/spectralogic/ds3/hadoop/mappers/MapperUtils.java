package com.spectralogic.ds3.hadoop.mappers;

import com.spectralogic.ds3.hadoop.Constants;
import org.apache.hadoop.mapred.JobConf;

import java.util.Map;

public class MapperUtils {
    public static void printJobConf(final JobConf conf) {
        System.out.println("Printing jobConf values:");
        for(final Map.Entry<String, String> jobConfValue: conf) {
            final String key = jobConfValue.getKey();
            if (key.equals(Constants.ACCESSKEY)) {
                System.out.println(key + ": <filtered>");
            }
            else {
                System.out.println(key + ": " + jobConfValue.getValue());
            }
        }
    }
}
