package com.iflytek.idmapping.hbase;

import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by admin on 2017/6/13.
 */
public class IDsHBaseValue {
    //"gloabl_id","imei","mac","imsi","phone_number","idfa","openudid","android_id"
    String global_id;
    Map<String,Integer> imei;
    Map<String,Integer> mac;
    Map<String,Integer> imsi;
    Map<String,Integer> phone_number;
    Map<String,Integer> idfa;
    Map<String,Integer> openudid;
    Map<String,Integer> android_id;

    public void init() {
        global_id = "";
        imei = new HashMap<>();
        mac = new HashMap<>();
        imsi = new HashMap<>();
        phone_number = new HashMap<>();
        idfa = new HashMap<>();
        openudid = new HashMap<>();
        android_id = new HashMap<>();
    }
    public String getGlobal_id() {
        return global_id;
    }

    public void setGlobal_id(String global_id) {
        this.global_id = global_id;
    }

    public Map<String, Integer> getImei() {
        return imei;
    }

    public void setImei(Map<String, Integer> imei) {
        this.imei = imei;
    }

    public Map<String, Integer> getMac() {
        return mac;
    }

    public void setMac(Map<String, Integer> mac) {
        this.mac = mac;
    }

    public Map<String, Integer> getImsi() {
        return imsi;
    }

    public void setImsi(Map<String, Integer> imsi) {
        this.imsi = imsi;
    }

    public Map<String, Integer> getPhone_number() {
        return phone_number;
    }

    public void setPhone_number(Map<String, Integer> phone_number) {
        this.phone_number = phone_number;
    }

    public Map<String, Integer> getIdfa() {
        return idfa;
    }

    public void setIdfa(Map<String, Integer> idfa) {
        this.idfa = idfa;
    }

    public Map<String, Integer> getOpenudid() {
        return openudid;
    }

    public void setOpenudid(Map<String, Integer> openudid) {
        this.openudid = openudid;
    }

    public Map<String, Integer> getAndroid_id() {
        return android_id;
    }

    public void setAndroid_id(Map<String, Integer> android_id) {
        this.android_id = android_id;
    }

    public Map<String,Integer> getIndexMap(int index) {
        if(index == 1) {
            return imei;
        } else if(index == 2) {
            return mac;
        } else if(index == 3) {
            return imsi;
        } else if(index == 4) {
            return phone_number;
        } else if(index == 5) {
            return idfa;
        } else if(index == 6) {
            return openudid;
        } else if(index == 7) {
            return android_id;
        }
        return null;
    }
    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
