package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by nitik on 4/14/2017.
 */

public class Request implements Serializable {
    private static final long serialVersionUID = 7526472295622776147L;
    String emuId;
    String joinerId;
    String requestType;
    String key;
    String value;
    String predEmuId;
    String succEmuId;
    Map<String,String> map;

    public Request(String emuId, String joinerId, String requestType,String key, String value,String predEmuId,String succEmuId,
                   Map<String,String> mapParam){
        this.emuId=emuId;
        this.joinerId=joinerId;
        this.requestType=requestType;
        this.key=key;
        this.value=value;
        this.predEmuId=predEmuId;
        this.succEmuId=succEmuId;
        this.map=mapParam;
    }
}
