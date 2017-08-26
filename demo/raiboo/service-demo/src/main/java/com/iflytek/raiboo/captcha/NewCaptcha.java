package com.iflytek.raiboo.captcha;

import com.iflytek.raiboo.Dbutil;
import com.iflytek.raiboo.SendMessage;
import jersey.repackaged.com.google.common.collect.Maps;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by jcao2014 on 2016/11/29.
 */
public class NewCaptcha {

    public String genCaptcha(String robot_id, String phonenumber) throws SQLException {

        Dbutil db = new Dbutil();
        String validpn = "true";
        int timestamp = (int) (System.currentTimeMillis() / 1000);
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        int date = Integer.parseInt(df.format(new Date()));
        String captcha = "";
        ResultSetHandler<Map<String, String>> h = new ResultSetHandler<Map<String, String>>() {
            public Map<String, String> handle(ResultSet rs) throws SQLException {
                Map<String, String> result = Maps.newHashMap();
                while (rs.next()) {
                    result.put(rs.getString(1), rs.getString(2));
                }
                return result;
            }
        };
        Map<String, String> result = null;
        ResultSetHandler<List<IsValid>> t = new BeanListHandler<IsValid>(IsValid.class);

        try {
            String sql = "SELECT captcha, phonenumber FROM raiboo_captcha_message where robot_id=" + robot_id + " and phonenumber=" + phonenumber + " and day=" + date;
            result = db.run.query(sql, h);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        if (result.size() > 0) {
            validpn = "false";
        }

        for (Map.Entry<String, String> entry : result.entrySet()) {
            captcha = entry.getKey();
            phonenumber = entry.getValue();
        }
        if (captcha == "" || captcha == null) {
            char[] chars = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0',
                    'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P',
                    'A', 'S', 'D', 'F', 'G', 'H', 'J', 'K', 'L',
                    'Z', 'X', 'C', 'V', 'B', 'N', 'M'};
            Random random = new Random();
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i <= 5; i++) {
                sb.append(chars[random.nextInt(chars.length)]);
            }
            captcha = sb.toString();
        }
        try {
            db.run.update(
                    "insert into raiboo_captcha_message (robot_id, captcha, phonenumber, isvalid, day, timestamp) values (?, ?, ?, ?, ?, ?)"
                    , robot_id, captcha, phonenumber, 1, date, timestamp
            );
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }



        String sql = "SELECT isvalid FROM raiboo_captcha_message where robot_id=" + robot_id
                + " and phonenumber=" + phonenumber
                + " and day=" + date;
        List<IsValid> valueList = null;
        try {
            valueList = db.run.query(sql, t);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        for (IsValid i : valueList)
            if (i.isvalid() == false) {
                validpn = "false";
                return validpn;
            }

        sql = "select count(*) from raiboo_captcha_message where isvalid=0 and phonenumber=" + phonenumber;
        int countNotValid = ((Long)db.run.query(sql, new ScalarHandler(1))).intValue();
        if(countNotValid >= 3) {
            validpn = "false";
            return validpn;
        }
        String msg = "尊敬的用户，您的验证码为: " + captcha + "。如非本人操作，敬请忽略。";
        SendMessage.SendCaptcha(msg, phonenumber);
        return validpn;
    }

}



//    public String getRandomString() {
//        char[] chars = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0',
//                'Q', 'W', 'E', 'R', 'T', 'Y', 'U', 'I', 'O', 'P',
//                'A', 'S', 'D', 'F', 'G', 'H', 'J', 'K', 'L',
//                'Z', 'X', 'C', 'V', 'B', 'N', 'M'};
//        Random random = new Random();
//        StringBuffer sb = new StringBuffer();
//        for (int i = 0; i <= 5; i++) {
//            sb.append(chars[random.nextInt(chars.length)]);
//        }
//        return sb.toString();
//    }


//       generate captcha


//      get phonenumber and cpatcha
//        ResultSetHandler<List<PhoneNumber>> h = new BeanListHandler<PhoneNumber>(PhoneNumber.class);
//        String sql = "SELECT phonenumber FROM raiboo_captcha_message where robot_id=" + robot_id + " and phonenumber=" + phonenumber + " and day=" + date;
//        List<PhoneNumber> pnList = null;
//        try {
//            pnList = db.run.query(sql, h);
//        } catch (SQLException sqle) {
//            sqle.printStackTrace();
//            result = "false";
////            return result;
//        }


//        send message
//        if (result == "true") {

//        for (PhoneNumber i : pnList) {
//            if (i.getPhonenumber() == phonenumber) {
//                result = "false";
//                return result;
//            }
//        }
//
//
//        ResultSetHandler<List<IsValid>> h = new BeanListHandler<IsValid>(IsValid.class);
//        String sql = "SELECT isvalid FROM raiboo_captcha_message where robot_id=" + robot_id + " and phonenumber=" + phonenumber + " and day=" + date;
//        List<IsValid> valueList = null;
//        try {
//            valueList = db.run.query(sql, h);
//        } catch (SQLException sqle) {
//            sqle.printStackTrace();
//        }
//        String result = "true";
//        if (valueList.size() == 0) {
//            result = "false";
//            return result;
//        }
//        for (IsValid i : valueList) {
//            if (i.isvalid() == false) {
//                result = "false";
//                return result;
//            }
//        }
//        return result;
//    }


//}
