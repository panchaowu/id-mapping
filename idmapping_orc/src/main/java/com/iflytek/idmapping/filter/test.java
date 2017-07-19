package com.iflytek.idmapping.filter;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by admin on 2017/6/19.
 */
public class test {

    public static void main(String[] args) {
        String globalID1 = MD5Hash.getMD5AsHex(Bytes.toBytes("nihaoma".toString().toUpperCase())).toUpperCase();
        String globalID2 = MD5Hash.getMD5AsHex(Bytes.toBytes("wobushihenhao".toString().toUpperCase())).toUpperCase();
        System.out.println(globalID1 + "\n" + globalID2);

        List<String> list = Collections.synchronizedList(new ArrayList<String>());
    }
}
