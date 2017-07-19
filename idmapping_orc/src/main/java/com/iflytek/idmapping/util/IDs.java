package com.iflytek.idmapping.util;

import com.google.gson.Gson;
import org.apache.hadoop.io.Text;

import java.util.List;
import java.util.Map;

/**
 * Created by admin on 2017/5/22.
 */
public class IDs {
    String global_id;
    List<Map<String,Integer> > dvcs;

    public String getGlobal_id() {
        return global_id;
    }

    public void setGlobal_id(String global_id) {
        this.global_id = global_id;
    }

    public List<Map<String, Integer>> getDvcs() {
        return dvcs;
    }

    public void setDvcs(List<Map<String, Integer>> dvcs) {
        this.dvcs = dvcs;
    }

    //    Map<Text,Integer> imei;
//    Map<Text,Integer> mac;
//    Map<Text,Integer> imsi;
//    Map<Text,Integer> phone_number;
//    Map<Text,Integer> idfa;
//    Map<Text,Integer> openudid;
//    Map<Text,Integer> android_id;
//
//    public String getGlobal_id() {
//        return global_id;
//    }
//
//    public void setGlobal_id(String global_id) {
//        this.global_id = global_id;
//    }
//
//    public Map<Text, Integer> getImei() {
//        return imei;
//    }
//
//    public void setImei(Map<Text, Integer> imei) {
//        this.imei = imei;
//    }
//
//    public Map<Text, Integer> getMac() {
//        return mac;
//    }
//
//    public void setMac(Map<Text, Integer> mac) {
//        this.mac = mac;
//    }
//
//    public Map<Text, Integer> getImsi() {
//        return imsi;
//    }
//
//    public void setImsi(Map<Text, Integer> imsi) {
//        this.imsi = imsi;
//    }
//
//    public Map<Text, Integer> getPhone_number() {
//        return phone_number;
//    }
//
//    public void setPhone_number(Map<Text, Integer> phone_number) {
//        this.phone_number = phone_number;
//    }
//
//    public Map<Text, Integer> getIdfa() {
//        return idfa;
//    }
//
//    public void setIdfa(Map<Text, Integer> idfa) {
//        this.idfa = idfa;
//    }
//
//    public Map<Text, Integer> getOpenudid() {
//        return openudid;
//    }
//
//    public void setOpenudid(Map<Text, Integer> openudid) {
//        this.openudid = openudid;
//    }
//
//    public Map<Text, Integer> getAndroid_id() {
//        return android_id;
//    }
//
//    public void setAndroid_id(Map<Text, Integer> android_id) {
//        this.android_id = android_id;
//    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public static void main(String[] args) {
//        IDs ids = new IDs();
//        ids.dvcs = new ArrayList<>();
//        ids.setGlobal_id("yw877ywe87we8e76qw86");
//        Map<Text,Integer> imei = new HashMap<>();
//        imei.put(new Text("723897238"),18);
//        ids.dvcs.add(imei);
//        System.out.println(ids.toString());
        String jsonStr = "{\"global_id\":\"mac_0c:91:60:46:85:a9mac_0c:91:60:46:85:a92086066514\",\"dvcs\":[{},{\"0c:91:60:46:85:a9\":0},{},{},{},{},{}]}";
        Gson gson = new Gson();
        IDs ids = gson.fromJson(jsonStr,IDs.class);
        System.out.println(ids.toString());
    }
}
