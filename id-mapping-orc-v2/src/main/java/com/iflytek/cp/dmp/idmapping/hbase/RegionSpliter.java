package com.iflytek.cp.dmp.idmapping.hbase;

import java.util.Arrays;

/**
 * 创建HBase表的region分裂预操作
 * region分类代价较大，主要使数据均匀落到各个region
 * 解决HBase导入过程中的region热点问题
 */
public class RegionSpliter {

    /*
    * region 分裂指定时，前置补0
    * 按length保证位数对齐
     */
    public static String padLeft(String s, int length)
    {
        byte[] bs = new byte[length];
        byte[] ss = s.getBytes();
        Arrays.fill(bs, (byte) (48 & 0xff));
        System.arraycopy(ss, 0, bs,length - ss.length, ss.length);
        return new String(bs);
    }

    /*
    * region 的前缀编码采用十六进制，startKey和endKey均为16进制，不宜过大
    * 可以保证够划分为numRegions个region即可
     */
    public static byte[][] getHexSplits(String startKey, String endKey,
                                        int numRegions) {
        byte[][] splits = new byte[numRegions - 1][];
        int lowestKey = Integer.parseInt(startKey,16);
        int highestKey = Integer.parseInt(endKey,16);
        int increment = (highestKey - lowestKey)/numRegions;
        for (int i = 0; i < numRegions - 1; i++) {
            int key = lowestKey + (increment * (i+1));
            String hexKey = padLeft(Integer.toHexString(key),4).toUpperCase();
            byte[] b = hexKey.getBytes();
            splits[i] = b;
        }
        return splits;
    }

}
