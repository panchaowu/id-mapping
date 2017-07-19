package com.iflytek.idmapping.format;

import com.google.gson.Gson;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.iflytek.idmapping.util.OrcUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Created by admin on 2017/5/25.
 */
public class OrcWriteTest {
    //{"gloabl_id","imei","mac","imsi","phone_number","idfa","openudid","android_id"}


    public static class ORCMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text keyTxt = new Text(key.toString());
            context.write(keyTxt,value);
        }
    }

    public static class ORCReducer extends Reducer<Text, Text, NullWritable, Writable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            OrcUtil.setupIDsOrc();
            OrcUtil.initialize(context);
        }

        private void setFieldMap(OrcStruct orcs,int index,Map<String,Integer> map) {
            Map<Text,IntWritable> orcMap = new HashMap<>();
            for(Map.Entry<String,Integer> e : map.entrySet()) {
                orcMap.put(new Text(e.getKey()),new IntWritable(e.getValue().intValue()));
            }
            OrcUtil.idsInspector.setStructFieldData(orcs,OrcUtil.idsFields.get(index),orcMap);
        }

        private final NullWritable nw = NullWritable.get();
        private Gson gson = new Gson();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            OrcStruct orcs = OrcUtil.InitIDs();
            for (Text val : values) {
                IDsDef ids = gson.fromJson(val.toString(), IDsDef.class);
                String globalId = ids.getGlobal_Id();
                Map<String,Integer> imei = ids.getImei();
                Map<String,Integer> mac = ids.getMac();
                Map<String,Integer> imsi = ids.getImsi();
                Map<String,Integer> phone = ids.getPhone_Number();
                Map<String,Integer> idfa = ids.getIdfa();
                Map<String,Integer> openudid = ids.getOpenudid();
                Map<String,Integer> androidId = ids.getAndroid_Id();
                OrcUtil.idsInspector.setStructFieldData(orcs,OrcUtil.idsFields.get(0),new Text(globalId));
                setFieldMap(orcs,1,imei);
                setFieldMap(orcs,2,mac);
                setFieldMap(orcs,3,imsi);
                setFieldMap(orcs,4,phone);
                setFieldMap(orcs,5,idfa);
                setFieldMap(orcs,6,openudid);
                setFieldMap(orcs,7,androidId);
                context.write(nw, OrcUtil.idsSerde.serialize(orcs,OrcUtil.idsInspector));
                }
            }
        }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //要设置结构，否则reduce会提示输入空值
        conf.set("orc.mapred.output.schema","struct<global_id:string,imei:map<string,int>,mac:map<string,int>,imsi:map<string,int>,phone_number:map<string,int>,idfa:map<string,int>,openudid:map<string,int>,android_id:map<string,int>>");
        Job job = new Job(conf, "ORCSample");
        job.setJarByClass(OrcWriteTest.class);
        job.setMapperClass(ORCMapper.class);
        job.setReducerClass(ORCReducer.class);
        //map类型设置
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //reduce类型设置
        job.setNumReduceTasks(1);
        job.setOutputFormatClass(OrcNewOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Writable.class);
        //输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
