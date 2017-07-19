package com.iflytek.idmapping.struct;

import com.google.gson.Gson;

import java.util.Map;

/**
 * Created by admin on 2017/6/27.
 */
public class IDsStruct {
    public String global_id;
    public Map<String,Integer> imei;
    public Map<String,Integer> mac;
    public Map<String,Integer> imsi;
    public Map<String,Integer> phone_number;
    public Map<String,Integer> idfa;
    public Map<String,Integer> openudid;
    public Map<String,Integer> android_id;

    public String getGlobal_Id() {
        return global_id;
    }

    public void setGlobal_Id(String global_Id) {
        this.global_id = global_Id;
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

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
