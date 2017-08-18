package com.iflytek.cp.dmp.idmapping.util;

import com.iflytek.cp.dmp.idmapping.struct.IDs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class IDMappingUtil {

    private static Set<String> validSrcSet = new HashSet<String>();
    private static Set<String> validTypeSet = new HashSet<String>();
    private static Set<String> srcWhiteSet = new HashSet<String>();
    private static Integer convergeMaxCount = 0;
    private static Configuration conf = new Configuration();
    private static Random random = null;

    static {
        conf.addResource("idmapping.xml");
        // 加载有效的源列表
        String[] srcs = conf.get("id.src.list", "vcoam,sdk,vc_up,mi_data,py_yun,cmcc,lingxi").split(",");
        validSrcSet.addAll(Arrays.asList(srcs));

        // 加载有效的id类型
        String[] types = conf.get("id.type.list", "imei,mac,imsi,idfa,openudid,phone_number,android_id").split(",");
        validTypeSet.addAll(Arrays.asList(types));

        // 加载数据源白名单
        String[] whiteSrcs = conf.get("id.clean.src.writelist", "cmcc,vc_up").split(",");
        srcWhiteSet.addAll(Arrays.asList(whiteSrcs));

        // 聚合的最大ID数量
        convergeMaxCount = conf.getInt("id.max.converge.count", 10);
    }

    public static Set<String> getValidSrcSet() {
        return validSrcSet;
    }

    public static Set<String> getValidTypeSet() {
        return validTypeSet;
    }

    public synchronized static String getRandomString(String taskId, int taskNum) {
        if(random == null) {
            String key = taskId.split("_")[4];
            random = new Random(Integer.valueOf(key));
        }
        return Integer.toString(random.nextInt(taskNum));
    }

    public static Set<String> getAntispamIDs() throws IOException {
        Set<String> res = new HashSet<String>();
        String filePath = conf.get("id.blacklist.hdfs.path", "hdfs://ns-hf/project/idmapping/idmapping/blacklist/blacklist.dat");
        FileSystem fs = FileSystem.get(URI.create(filePath), conf);
        FileStatus[] fss = fs.listStatus(new Path(filePath));
        for (FileStatus fstatus : fss) {
            if(fstatus.isFile()) {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(fstatus.getPath())));
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    res.add(line);
                }
                bufferedReader.close();
            }
        }
        return res;
    }

    /* 第一个参数是反作弊ID的集合
    *  第二个参数是过期的时间戳
    * */
    public static void filterIDs(IDs ids, Set<String> antiList, Integer cleanDatetime) {
        boolean isDeleted;
//        if(ids.ids.size() == 0) {
//            return;
//        }
        for (String type : validTypeSet) {
            Map<String, IDs.Info> idlist = ids.ids.get(type);
            List<String> idTmpList = new ArrayList<String>();
            for(String str: idlist.keySet()) {
                idTmpList.add(str);
            }
            for (String str : idTmpList) {
                isDeleted = false;
                if (antiList != null && antiList.contains(str)) {
                    idlist.remove(str);
                    continue;
                }
                // 如果ID过期并且不在白名单里，则删除对应的ID
                if (idlist.get(str).datetime < cleanDatetime) {
                    isDeleted = true;
                    String[] srcs = idlist.get(str).src.split(",");
                    for (String src : srcs) {
                        if (srcWhiteSet.contains(src)) {
                            isDeleted = false;
                            break;
                        }
                    }
                }
                if (isDeleted) {
                    idlist.remove(str);
                }
            }
        }
    }

    // 获取全部id列表，以类型开头，用来做聚合
    public static TreeSet<String> getAllIdSet(IDs ids) {
        TreeSet<String> ret = new TreeSet<String>();
        for (String idType : ids.ids.keySet()) {
            Map<String, IDs.Info> map = ids.ids.get(idType);
            for (String str : map.keySet()) {
                ret.add(idType + "_" + str);
            }
        }
        return ret;
    }

    // 返回true代表返回失败
    public static boolean convergeID(IDs dIDs, IDs sIDs, boolean isCheckOverCapacity) {
        String idKey;
        IDs.Info idInfo;
        for(String type : validTypeSet) {
            Map<String, IDs.Info> dTmpMap = dIDs.ids.get(type);
            Map<String, IDs.Info> sTmpMap = sIDs.ids.get(type);
            for(Map.Entry<String, IDs.Info> entrySet : sTmpMap.entrySet()) {
                idKey = entrySet.getKey();
                idInfo = entrySet.getValue();
                // 如果目标IDs不存在,则加入
                if (!dTmpMap.containsKey(idKey) ) {
                    dTmpMap.put(idKey, idInfo);
                } else {
                    // 更新时间
                    if (dTmpMap.get(idKey).datetime < idInfo.datetime) {
                        dTmpMap.get(idKey).datetime = idInfo.datetime;
                        dTmpMap.get(idKey).model = idInfo.model;
                    }
                    // 更新src
                    StringBuffer src = new StringBuffer();
                    Set<String> srcSet = new HashSet<String>(Arrays.asList(dTmpMap.get(idKey).src.split(",")));
                    srcSet.addAll(Arrays.asList(idInfo.src.split(",")));
                    for(String tmp : srcSet) {
                        src.append(tmp);
                        src.append(",");
                    }
                    idInfo.src = src.substring(0, src.length()-1);
                    dTmpMap.put(idKey, idInfo);
                }
            }
            if (isCheckOverCapacity && dTmpMap.size() > convergeMaxCount) {
                return true;
            }
        }
        return false;
    }
}
