package com.iflytek.raiboo.captcha;

import java.sql.SQLException;

import com.iflytek.raiboo.Dbutil;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * Created by jcao2014 on 2016/11/29.
 */
public class NewCaptcha {

    public String getRandomString() {
        char[] chars = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0',
                'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P',
                'A', 'S', 'D', 'F', 'G', 'H', 'J', 'K', 'L',
                'Z', 'X', 'C', 'V', 'B', 'N', 'M'};
        Random random = new Random();
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i <= 5; i++) {
            sb.append(chars[random.nextInt(chars.length)]);
        }
        return sb.toString();
    }

    public String genCaptcha(String robot_id, String phonenumber) throws SQLException {
        NewCaptcha nc = new NewCaptcha();
        Dbutil db = new Dbutil();
        String code = nc.getRandomString();
        int timestamp = (int)(System.currentTimeMillis()/1000);
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        int date = Integer.parseInt(df.format(new Date()));
        try {
            db.run.update("insert into raiboo_captcha_message (robot_id, captcha, phonenumber, isvalid, day, timestamp) values (?, ?, ?, ?, ?, ?)",
                    robot_id, code, phonenumber, 1, date, timestamp);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        return code;
    }


}
