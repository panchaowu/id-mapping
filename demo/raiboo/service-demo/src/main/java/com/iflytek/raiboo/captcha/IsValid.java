package com.iflytek.raiboo.captcha;

/**
 * Created by jcao2014 on 2016/11/30.
 */
public class IsValid {
    private boolean isvalid;

    @Override
    public String toString() {
        return "IsValid{" +
                "isvalid=" + isvalid +
                '}';
    }

    public boolean isvalid() {
        return isvalid;
    }

    public void setIsvalid(boolean isvalid) {
        this.isvalid = isvalid;
    }
}
