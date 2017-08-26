package com.iflytek.raiboo;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;


/**
 * Created by admin on 2016/11/30.
 */
public class SendMessage {

//    static Set<String> phoneNumbers = new HashSet<String>();
    static String url = "http://sms.openspeech.cn/api/send";

    public static void SendCaptcha(String text, String phoneNumber) {
        String getString = "apikey=1579bad34c82628d274cedc56ebe12dd&" +
                "password=zhyx54321&templateid=10949&templateparams={\"content\":\"" + text + "\"}&mobile=" + phoneNumber;
//        String url =
        SendMessage.sendGet(url, getString);
    }

    public static void SendPromotion(String text, String phoneNumber) {
//        phoneNumbers.add(phoneNumber.trim());
//        if(phoneNumbers.size()==50){
//            for(String i: phoneNumbers){
//                phoneNumber = i + ",";
//            }
//            phoneNumber = phoneNumber.substring(0,phoneNumber.length()-1);
            String getString = "apikey=1579bad34c82628d274cedc56ebe12dd&" +
                    "password=zhyx54321&templateid=10963&templateparams={\"content\":\"" + text + "\"}&mobile=" + phoneNumber;
//            String url = "http://192.168.72.3/api/send";
            SendMessage.sendGet(url, getString);
//            phoneNumbers = new HashSet<String>();
//        }

    }


    /**
     * 向指定URL发送GET方法的请求
     *
     * @param url   发送请求的URL
     * @param param 请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
     * @return URL 所代表远程资源的响应结果
     */
    private static String sendGet(String url, String param) {
        String result = "";
        BufferedReader in = null;
        try {
            String urlNameString = url + "?" + param;
            URL realUrl = new URL(urlNameString.replace(" ", "").trim());
            // 打开和URL之间的连接
            URLConnection connection = realUrl.openConnection();
            // 设置通用的请求属性
//            connection.setRequestProperty("accept", "*/*");
//            connection.setRequestProperty("connection", "Keep-Alive");
//            connection.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 建立实际的连接
            connection.connect();
            // 获取所有响应头字段
            Map<String, List<String>> map = connection.getHeaderFields();
            // 遍历所有的响应头字段
            for (String key : map.keySet()) {
                System.out.println(key + "--->" + map.get(key));
            }
            // 定义 BufferedReader输入流来读取URL的响应
            in = new BufferedReader(new InputStreamReader(
                    connection.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送GET请求出现异常！" + e);
            e.printStackTrace();
        }
        // 使用finally块来关闭输入流
        finally {
            try {
                if (in != null)  {
                    in.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return result;
    }


//    public static void main(String[] args) {
//        //发送 GET 请求
//        SendMessage.SendMsg("这是一个测试", "18326160626");
//    }
}