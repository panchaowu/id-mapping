package com.iflytek.idmapping.format;

import com.google.gson.Gson;

import java.util.Map;

/**
 * Created by admin on 2017/5/25.
 */
public class IDsDef {
    public String Global_Id;
    public Map<String,Integer> Phone_Number;
    public Map<String,Integer> Mac;
    public Map<String,Integer> Imsi;
    public Map<String,Integer> Openudid;
    public Map<String,Integer> Android_Id;
    public Map<String,Integer> Idfa;
    public Map<String,Integer> Imei;
    public Map<String,Integer> Uid;
    public Map<String,Integer> Did;

    public String getGlobal_Id() {
        return Global_Id;
    }

    public void setGlobal_Id(String global_Id) {
        Global_Id = global_Id;
    }

    public Map<String, Integer> getPhone_Number() {
        return Phone_Number;
    }

    public void setPhone_Number(Map<String, Integer> phone_Number) {
        Phone_Number = phone_Number;
    }

    public Map<String, Integer> getMac() {
        return Mac;
    }

    public void setMac(Map<String, Integer> mac) {
        Mac = mac;
    }

    public Map<String, Integer> getImsi() {
        return Imsi;
    }

    public void setImsi(Map<String, Integer> imsi) {
        Imsi = imsi;
    }

    public Map<String, Integer> getOpenudid() {
        return Openudid;
    }

    public void setOpenudid(Map<String, Integer> openudid) {
        Openudid = openudid;
    }

    public Map<String, Integer> getAndroid_Id() {
        return Android_Id;
    }

    public void setAndroid_Id(Map<String, Integer> android_Id) {
        Android_Id = android_Id;
    }

    public Map<String, Integer> getIdfa() {
        return Idfa;
    }

    public void setIdfa(Map<String, Integer> idfa) {
        Idfa = idfa;
    }

    public Map<String, Integer> getImei() {
        return Imei;
    }

    public void setImei(Map<String, Integer> imei) {
        Imei = imei;
    }

    public Map<String, Integer> getUid() {
        return Uid;
    }

    public void setUid(Map<String, Integer> uid) {
        Uid = uid;
    }

    public Map<String, Integer> getDid() {
        return Did;
    }

    public void setDid(Map<String, Integer> did) {
        Did = did;
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
