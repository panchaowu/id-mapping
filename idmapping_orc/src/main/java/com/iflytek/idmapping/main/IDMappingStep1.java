package com.iflytek.idmapping.main;
import com.iflytek.idmapping.util.FileUtil;
import com.iflytek.idmapping.util.IdMappingUtil;
import com.iflytek.idmapping.util.OrcUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MRVersion;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by admin on 2017/5/4.
 */
public class IDMappingStep1 implements Tool {
    public static final Log log = LogFactory.getLog(IDMappingStep1.class);

    private Configuration conf = new Configuration();

    public static class Step1M extends Mapper<NullWritable, Writable, Text, Text> {
        private HashSet<String> invalidDvcSet;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            OrcUtil.setupIDsOrc();
            OrcUtil.initialize(context);
            invalidDvcSet = new HashSet<>();
            FileUtil.loadDataToSet("/project/idmapping/idmapping/filter_dvc/collected_dvc",
                    invalidDvcSet);
            System.out.println("invalid dvc set size :" + invalidDvcSet.size());
        }

        @Override
        protected void map(NullWritable key, Writable value1, Context context) throws IOException, InterruptedException {

            OrcStruct srcOrc = (OrcStruct)value1;
          //  OrcStruct value = OrcUtil.toLowerCase(srcOrc);
            OrcStruct value = OrcUtil.toLowerCaseAndClean(srcOrc,invalidDvcSet);
            Map<String,Integer> ids = OrcUtil.mergeIDsToMap(value);
            OrcUtil.setIDsGlobalId(value,"");
            if (ids != null && ids.size() != 0) {
                String secondKey = (String)ids.keySet().toArray()[0] + ids.keySet().toArray()[ids.keySet().size() - 1]
                        + IdMappingUtil.getRandomString(context.getTaskAttemptID().toString(), Integer.MAX_VALUE);
                OrcUtil.setIDsGlobalId(value,secondKey);
                String IDsStr = IdMappingUtil.orc2Json(value);
                for (Map.Entry<String, Integer> entry : ids.entrySet()) {
                    String idValue = entry.getKey();
                  //  context.write(new Text(idValue), OrcUtil.idsSerde.serialize(value,OrcUtil.idsInspector));
                    context.write(new Text(idValue),new Text(IDsStr));
                }
            }
        }
    }

    public static class Step1R extends Reducer<Text,Text, NullWritable, Writable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            OrcUtil.setupIDsOrc();
            OrcUtil.initialize(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            OrcStruct sumOrc = OrcUtil.InitIDs();
            Boolean isFadeIds = false;
            ArrayList<String> globalIdList = new ArrayList<>();
            int keyCount = 0;
            for (Text val1 : values) {
              //  OrcStruct val = (OrcStruct)val1;
                OrcStruct val = IdMappingUtil.json2orc(val1.toString());
                context.write(null, OrcUtil.idsSerde.serialize(val,OrcUtil.idsInspector));
                String globalId = OrcUtil.getIDsGlobalId(val);
                if(globalIdList.contains(globalId) == false) {
                    globalIdList.add(globalId);
                }
                OrcUtil.addIDs(val,sumOrc);
                if(OrcUtil.isFakeIDs(sumOrc)) {
                    isFadeIds = true;
                    break;
                }
                keyCount++;
                if(keyCount > 10000) {
                    break;
                }
            }
            // 山寨设备号
            if(isFadeIds) {
                for(Text val1 : values) {
                   // OrcStruct val = (OrcStruct)val1;
                    OrcStruct val = IdMappingUtil.json2orc(val1.toString());
                    context.write(null,OrcUtil.idsSerde.serialize(val,OrcUtil.idsInspector));
                }
            } else {
                for (String globalId : globalIdList) {
                    OrcUtil.setIDsGlobalId(sumOrc,globalId);
                    context.write(null,OrcUtil.idsSerde.serialize(sumOrc,OrcUtil.idsInspector));
                }
            }
        }
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(conf);
        String input = args[0];
        String output = args[1];
        System.out.println("Input:" + input);
        System.out.println("Output:" + output);
        //输入输出路径
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        //map类型设置
        job.setInputFormatClass(OrcNewInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
     //   job.setMapOutputValueClass(Writable.class);
        job.setMapOutputValueClass(Text.class);
    //    job.setMapOutputValueClass(OrcStruct.class);
        //reduce类型设置
        job.setOutputFormatClass(OrcNewOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Writable.class);
     //   job.setOutputValueClass(OrcStruct.class);
        job.setJarByClass(IDMappingStep1.class);
        job.setMapperClass(Step1M.class);
        job.setReducerClass(Step1R.class);
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new IDMappingStep1(), args);
        System.exit(res);
    }
}
