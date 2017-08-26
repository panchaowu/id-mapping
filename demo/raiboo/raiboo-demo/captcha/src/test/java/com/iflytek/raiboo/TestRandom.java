package com.iflytek.raiboo;

/**
 * Created by jcao2014 on 2016/11/29.
 */

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class TestRandom {

    public static void main(String[] args) {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");//设置日期格式
        System.out.println(df.format(new Date()));// new Date()为获取当前系统时间
    }
}