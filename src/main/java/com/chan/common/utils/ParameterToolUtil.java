package com.chan.common.utils;

import com.chan.common.constant.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class ParameterToolUtil {

    public static ParameterTool createParameterTool(final String[] args) throws IOException {
        return ParameterTool
                .fromPropertiesFile(ParameterToolUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                .mergeWith(ParameterTool.fromArgs(args))
                .mergeWith(ParameterTool.fromSystemProperties())
                .mergeWith(ParameterTool.fromMap(getenv()));
    }

    public static ParameterTool createParameterTool() throws IOException {
        return ParameterTool
                .fromPropertiesFile(ParameterToolUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                .mergeWith(ParameterTool.fromSystemProperties())
                .mergeWith(ParameterTool.fromMap(getenv()));
    }

    private static Map<String, String> getenv() {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }
}
