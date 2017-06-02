package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;

/**
 * Created by Haril Satra on 4/5/2017.
 */

public class Node implements Serializable{
    String info;
    String port;
    String key;
    String value;
    String msg;
    String hash;
    String query;
    String query_result;
}
