package com.iflytek.raiboo.promotion;

import com.iflytek.raiboo.idmapping.IDMappingClient2;
import com.iflytek.raiboo.ids.IDs;

import java.io.IOException;
import java.util.Map;

/**
 * Created by jcao2014 on 2016/12/2.
 */
public class Mac2Phonenumber {

    private IDMappingClient2 _idMappingClient = new IDMappingClient2();
    private boolean initted = false;

    public void init() {
        if (initted == true) {
            return;
        }
        try {
            _idMappingClient.init();
            System.out.println("idmapping inited");
            initted = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            _idMappingClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        initted = false;
    }

    public String getPhonenumber(String mac) throws IOException, InterruptedException {
        String phonenumber = "";
        if (mac == null || mac.length() == 0) {
            return phonenumber;
        }
        IDs ids = _idMappingClient.getIDs(mac);
        try {
            if (ids.getPhoneNumber() != null) for (Map.Entry<String, Integer> entry : ids.getPhoneNumber().entrySet()) {
                phonenumber = entry.getKey();
            }
        } catch (Exception e) {
            e.printStackTrace();
            _idMappingClient.close();
            _idMappingClient.init();
            System.err.println("Id mapping get ids failed!");
        }
        return phonenumber;
    }

}
