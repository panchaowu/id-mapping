package com.iflytek.raiboo;

import com.google.gson.Gson;
import com.iflytek.raiboo.api.TagInsightApi;
import com.iflytek.raiboo.captcha.NewCaptcha;
import com.iflytek.raiboo.captcha.UserStats;
import com.iflytek.raiboo.captcha.Verify;
import com.iflytek.raiboo.promotion.Mac2Phonenumber;
import com.iflytek.raiboo.promotion.Promotion;
import com.iflytek.raiboo.questionare.UpdateStartEnd;
import com.iflytek.raiboo.utils.Constants;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Root resource (exposed at "raiboo" path)
 */
@Path("/raiboo")
public class RaibooService {

    private static FlowAnalysis fa = null;
    private Gson gson = new Gson();
    private static Mac2Phonenumber mp = null;
    private static TagInsightApi tagApi = null;
    private static Set<String> phoneNumbers = new HashSet<String>();

    static {
        fa = new FlowAnalysis();
        mp = new Mac2Phonenumber();
        mp.init();
        tagApi = new TagInsightApi();
        tagApi.Initialize();
    }


    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "text/plain" media type.
     * <p>
     * <p>
     * curl "http://raiboo.xfyun.cn/raiboo/new?phonenumber=18226618013&robot_id=0"
     * curl "http://raiboo.xfyun.cn/raiboo/verify?captcha=YWICES"
     * curl "http://raiboo.xfyun.cn/raiboo/promote?mac=14:1a:a3:90:1f:c0&market_id=4"
     * curl "http://raiboo.xfyun.cn/raiboo/flowAnalysis?type=week&granularity=hour&market_id=4"
     * curl "http://raiboo.xfyun.cn/raiboo/taglist?date=quarter&market_id=-1&type_id=2"
     * curl "http://raiboo.xfyun.cn/raiboo/gpsdata?date=week&market_id=ALL&type_id=ALL"
     * curl "http://raiboo.xfyun.cn/raiboo/marketid"
     * curl "http://raiboo.xfyun.cn/raiboo/userStats"
     * curl "http://raiboo.xfyun.cn/raiboo/analysis/{mid}"
     * curl -i -XGET  'http://raiboo.xfyun.cn/analysis/ALL'
     * curl -i -XPOST -H 'Content-Type: text/plain' 'http://raiboo.xfyun.cn/question' -d'{"robot_id": "4@8@0@1"}'
     *
     * @return String that will be returned as a text/plain response.
     */

    @GET
    @Path("/new")
    @Produces("application/json")
    public String newCaptcha(@QueryParam("phonenumber") String phonenumber, @QueryParam("robot_id") String robot_id) throws SQLException {
        if (phonenumber == null) phonenumber = "";
        if (robot_id == null) robot_id = "";
        NewCaptcha nc = new NewCaptcha();
//        String captcha = nc.getRandomString();
        phonenumber = phonenumber.trim();
        System.out.println("NewCaptchaPhoneNubmer========" + phonenumber);
        String result = nc.genCaptcha(robot_id, phonenumber);
        UpdateStartEnd use = new UpdateStartEnd();
        use.updateEnd(robot_id);
        System.out.println(result);
        return result;
    }

    @GET
    @Path("/verify")
    @Produces(MediaType.TEXT_PLAIN)
    public String verify(@QueryParam("captcha") String captcha) throws SQLException {

        if (captcha == null) captcha = "";
        String validation = "true";
        Map<String, String> result = new HashMap<>();
        Verify v = new Verify();
        try {
            result = v.getRobotidPhonenubmer(captcha);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
            validation = "false";
        }

        try {
            validation = v.isValid(result, captcha);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
            validation = "false";
        }
        System.out.println(validation);
        return validation;
    }

    @GET
    @Path("/promote")
    @Produces(MediaType.TEXT_PLAIN)
    public String promotion(@QueryParam("mac") String mac, @QueryParam("market_id") String market_id) throws SQLException, IOException, InterruptedException {

//        System.out.println(mac + "============" + market_id);
        if (mac == null) mac = "";
        if (market_id == null) market_id = "";
        String phonenumber = "";
        try {
//            Mac2Phonenumber mp = new Mac2Phonenumber();
//
//            System.out.println("idmaping start");
            phonenumber = mp.getPhonenumber(mac.trim());
            System.out.println("phonenumber===============" + phonenumber);
        } catch (Exception e) {
            e.printStackTrace();
//            String phonenumber = "";
        }
        try {
            Promotion p = new Promotion();
            Map<Long, Integer> marketBrandId = p.readDB(market_id);
            if (phonenumber != "") {
                p.updateDB(mac, phonenumber, marketBrandId);
                phoneNumbers.add(phonenumber.trim());
                SimpleDateFormat df = new SimpleDateFormat("mm");
                if (phoneNumbers.size() == 10 || Integer.parseInt(df.format(new Date()))%5 == 0) {
                    for (String i : phoneNumbers) {
                        phonenumber = i + ",";
                    }
                    phonenumber = phonenumber.substring(0, phonenumber.length() - 1);
                    String msg = "让世界聆听我们的声音。";
                    SendMessage.SendPromotion(msg, phonenumber);
                    phoneNumbers = new HashSet<String>();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "done";
    }


    @GET
    @Path("/flowAnalysis")
    @Produces("application/json")
    public String flowAnalysis(@QueryParam("type") String type, @QueryParam("granularity") String granularity, @QueryParam("market_id") String market_id) throws SQLException, ParseException, CloneNotSupportedException {
        System.out.println("===flowAnalysis");
        String result = "";
        if (type == null) type = "";
        if (granularity == null) granularity = "";
        if (market_id == null) market_id = "";
//        FlowAnalysis fa = new FlowAnalysis();
        System.out.println("type=" + type + "granularity=" + granularity + "market_id=" + market_id);
        try {
//            fa.init();
//            tagApi.Initialize();
            result = fa.getRealTimeFlow(type, granularity, market_id);
            System.out.println("flowAnalysis  " + result);
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(result);
        return result;
    }

    @GET
    @Path("/taglist")
    @Produces("application/json")
    public String taglist(@QueryParam("date") String date, @QueryParam("market_id") String market_id, @QueryParam("type_id") String type_id) throws SQLException {

        if (date == null) date = "";
        if (date.trim().toLowerCase().equals(Constants.WEEK)) {
            date = Constants.WEEK;
        } else if (date.trim().toLowerCase().equals(Constants.MONTH)) {
            date = Constants.MONTH;
        } else if (date.trim().toLowerCase().equals(Constants.QUARTER)) {
            date = Constants.QUARTER;
        } else {
            throw new IllegalArgumentException("Date value error!");
        }
        if (market_id == null) market_id = "";
        if (type_id == null) type_id = "";
        System.out.println("date=" + date + "market_id=" + market_id + "type_id" + type_id);


        try {

//            System.out.println("taglistInit");

            return gson.toJson(tagApi.GetTagList(date, market_id, type_id));
        } catch (Exception e) {
            e.printStackTrace();
            return gson.toJson("[]");
        }

    }

    @GET
    @Path("/gpsdata")
    @Produces("application/json")
    public String gpsdata(@QueryParam("date") String date, @QueryParam("market_id") String market_id, @QueryParam("type_id") String type_id) throws SQLException {

        if (date == null) date = "";

        if (date.trim().toLowerCase().equals(Constants.WEEK)) {
            date = Constants.WEEK;
        } else if (date.trim().toLowerCase().equals(Constants.MONTH)) {
            date = Constants.MONTH;
        } else if (date.trim().toLowerCase().equals(Constants.QUARTER)) {
            date = Constants.QUARTER;
        } else {
            throw new IllegalArgumentException("Date value error!");
        }

        if (market_id == null) market_id = "";
        if (type_id == null) type_id = "";
        System.out.println("date=" + date + "market_id=" + market_id + "type_id" + type_id);

//        TagInsightApi tagApi = new TagInsightApi();
        try {
//a
//            System.out.println("gpsdataInit");
            String gd = gson.toJson(tagApi.GetGpsData(Constants.WEEK, market_id, type_id));
            System.out.println(gd);
            return gd;
        } catch (Exception e) {
            e.printStackTrace();
            return gson.toJson("[]");
        }

    }

    @GET
    @Path("/marketid")
    @Produces("application/json")
    public String marketID() throws SQLException, ParseException {
        try {
            return gson.toJson(fa.getMarketWithID());
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    @GET
    @Path("/userStats")
    @Produces("application/json")
    public String userStats() throws SQLException, ParseException {
        UserStats us = new UserStats();
        Map<String,Integer> count = us.count();
        return gson.toJson(count);
    }
}
