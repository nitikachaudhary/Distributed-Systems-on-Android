package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by nitik on 4/30/2017.
 */

public class Request implements Serializable {
    private static final long serialVersionUID = 7526472295622776147L;
    String port;
    String requestType;
    String key;
    String value;
    Map<String,String> map;

    public Request(String port, String requestType,String key, String value, Map<String,String> mapParam){
        this.port = port;
        this.requestType = requestType;
        this.key = key;
        this.value = value;
        this.map=mapParam;
    }
}
