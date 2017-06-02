package edu.buffalo.cse.cse486586.simpledht;

import java.util.Comparator;

/**
 * Created by Haril Satra on 4/8/2017.
 */

public class NodeComparator implements Comparator<Node> {
    @Override
    public int compare(Node x, Node y){
        if (x.hash.compareTo(y.hash) < 0)
        {
            return -1;
        }
        if (x.hash.compareTo(y.hash) > 0)
        {
            return 1;
        }
        return 0;
        //http://stackoverflow.com/questions/4064633/string-comparison-in-java
        //return x.key.compareTo(y.key);
    }
}
