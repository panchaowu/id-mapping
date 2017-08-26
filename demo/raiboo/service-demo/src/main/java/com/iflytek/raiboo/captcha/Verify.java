package com.iflytek.raiboo.captcha;


import com.iflytek.raiboo.Dbutil;
import jersey.repackaged.com.google.common.collect.Maps;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by jcao2014 on 2016/11/29.
 */
public class Verify {

    public Map<String, String> getRobotidPhonenubmer(String captcha) throws SQLException {
        Dbutil db = new Dbutil();
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
        try {
            result = db.run.query("select robot_id, phonenumber from raiboo_captcha_message where captcha=?", h, captcha);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        return result;
    }

    public String isValid(Map<String, String> phonenumberDay, String captcha) throws SQLException {
        if (phonenumberDay.size()==0 || phonenumberDay==null){
            return "false";
        }
        Dbutil db = new Dbutil();
        ResultSetHandler<List<IsValid>> h = new BeanListHandler<IsValid>(IsValid.class);

        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        int day = Integer.parseInt(df.format(new Date()));
        String robotid = "0", phonenumber = "0";
        for (Map.Entry<String, String> entry : phonenumberDay.entrySet()) {
            robotid = entry.getKey();
            phonenumber = entry.getValue();
        }
        String sql = "SELECT isvalid FROM raiboo_captcha_message where robot_id=" + robotid
                + " and phonenumber=" + phonenumber
                + " and day=" + day;
//                + " and captcha=" + captcha.trim();
        List<IsValid> valueList = null;
        String result = "true";
        try {
            valueList = db.run.query(sql, h);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
            result = "false";
            return result;
        }
        if (valueList.size() == 0) {
            result = "false";
            return result;
        }
        for (IsValid i : valueList)
            if (i.isvalid() == false) {
                result = "complete";
                return result;
            }
        String updateSql = "UPDATE raiboo_captcha_message SET isvalid=0 WHERE captcha='" + captcha + "'";
        try {
            db.run.update(updateSql);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
        return result;
    }

}
