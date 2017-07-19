package com.iflytek.idmapping.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by admin on 2017/6/14.
 */
public class FileUtil {
    public static void loadDataToSet(String rFile, HashSet<String> invalidSet) throws IOException {
        Configuration conf = new Configuration();
        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        String lineTxt = null;
        FileSystem fs = FileSystem.get(URI.create(rFile),conf);
        fsr = fs.open(new Path(rFile));
        bufferedReader = new BufferedReader(new InputStreamReader(fsr));
        while ((lineTxt = bufferedReader.readLine()) != null) {
            invalidSet.add(lineTxt);
        }
        bufferedReader.close();
    }

    public static List<String> getSubDirs(String dir) throws IOException {
        List<String> subDirList = new ArrayList<>();
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create(dir), conf);
        FileStatus[] fss = hdfs.listStatus(new Path(dir));
        for(FileStatus fs : fss) {
            if(fs.isDirectory()) {
                subDirList.add(fs.getPath().toString());
            }
        }
        return subDirList;
    }
}
