package com.iflytek.cp.dmp.idmapping.mr;

import com.iflytek.cp.dmp.idmapping.struct.IDs;
import com.iflytek.cp.dmp.idmapping.util.IDMappingUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class IDMappingStep1  implements Tool {
    private Configuration conf = new Configuration();

    public static class Step1M extends Mapper<NullWritable, OrcStruct, Text, OrcValue> {

        private OrcValue orcValue;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            orcValue = new OrcValue();
        }

        /*  idmapping step I mapper class
        *   1) 将global_id初始化为空;
        *   2) 如果该ID size > 0，则选取保存到global_id作为step II的key做还原
        *   3) 每个ID作为key，输出一遍，用于聚合
        *   4) 会根据src、datetime、src白名单做过滤
        * */
        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
            IDs ids = new IDs();
            // 小于cleanDatetime的ID会过滤
            Integer cleanDatetime = context.getConfiguration().getInt("idmapping.clean.date", 0);
            // 转化后ids里的所有ID都会变成小写
            ids.fromOrcStruct(value, true);
            // 过滤过期ID
            IDMappingUtil.filterIDs(ids, null, cleanDatetime);
            TreeSet<String> allIDs = IDMappingUtil.getAllIdSet(ids);
            if (allIDs.size() == 0) {
                return;
            } else {
                ids.globalID = (String) allIDs.toArray()[0] + allIDs.toArray()[allIDs.size() - 1] +
                        IDMappingUtil.getRandomString(context.getTaskAttemptID().toString(), Integer.MAX_VALUE);
                orcValue.value = ids.toOrcStruct();
            }
            // 过滤后没有ID的话不再输出
            for(String tmpID : allIDs) {
                context.write(new Text(tmpID), orcValue);
            }
        }
    }

    /* idmapping step I reduce class
    *  补全相同ID对应的全部ID，不去重，去重在step II
    *  注：对ID为空值处理，不处理直接输出，使用random做key来保证负载平衡
    * */
    public static class Step1R extends Reducer<Text, OrcValue, NullWritable, OrcStruct> {

        private List<String> antispamIDList = new ArrayList<String>();

        protected void reduce(Text key, Iterable<OrcValue> values, Context context) throws IOException, InterruptedException {

            IDs newIDs = new IDs();
            Set<String> secondKeys = new TreeSet<String>();
            // 转化为IDs结构，方便处理
            boolean isOverCapacity = false;
            for (OrcValue orcValue: values) {
                IDs ids = new IDs();
                OrcStruct orcStruct = (OrcStruct) orcValue.value;
                ids.fromOrcStruct(orcStruct,false);
                if (!isOverCapacity) {
                    String tmpGlobalID = ids.globalID;
                    // 加入secondKeys用来作为输出
                    secondKeys.add(tmpGlobalID);
                    // 聚合数据，超过10个不做聚合,直接丢弃
                    // 如果超出容量，isOverCapacity设置为true
                    if(IDMappingUtil.convergeID(newIDs, ids, true)) {
                        isOverCapacity = true;
                        antispamIDList.add(key.toString());
                        break;
                    }
                }
            }
            if (!isOverCapacity) {
                for (String secondKey : secondKeys) {
                    newIDs.globalID = secondKey;
                    context.write(NullWritable.get(), newIDs.toOrcStruct());
                }
            }
//            // SecondKeys是用来输出用的，10条数据聚合后还能输出10条数据
//
//            // 聚合数据，超过10个不做聚合，为了保证不丢数据，原始数据也会输出
//            // 不保存，目的是处理异常情况，解决oom问题
//            for (IDs ids : idss) {
//                context.write(NullWritable.get(), ids.toOrcStruct());
//                if (!isOverCapacity) {
//                    String tmpGlobalID = ids.globalID;
//                    // 加入secondKeys用来作为输出
//                    secondKeys.add(tmpGlobalID);
//                    // 如果超出容量，isOverCapacity设置为true
//                    if(IDMappingUtil.convergeID(newIDs, ids, true)) {
//                        isOverCapacity = true;
//                        antispamIDList.add(key.toString());
//                    }
//                }
//            }
//            if (!isOverCapacity) {
//                for (String secondKey : secondKeys) {
//                    newIDs.globalID = secondKey;
//                    context.write(NullWritable.get(), newIDs.toOrcStruct());
//                }
//            }
        }

        /* 将作弊ID写入到反作弊目录里 */
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 如果没有作弊ID就返回
            if (antispamIDList.size() == 0) {
                return;
            }
            String antiPathString = context.getConfiguration().get("id.blacklist.hdfs.path",
                    "hdfs://ns-hf/project/idmapping/idmapping/blacklist/");
            String outputFile = antiPathString + "/" + context.getTaskAttemptID().toString();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputFile))));
            for (String str : antispamIDList) {
                bufferedWriter.write(str.substring(str.indexOf("_") + 1) + "\n");
            }
            bufferedWriter.close();
            super.cleanup(context);
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Arguments error, [input] [output] [clean.date]");
            System.exit(-1);
        }

        String input = args[0];
        String output = args[1];
        String cleanDate = args[2];
        System.out.println(String.format("idmapping step 1\n input:%s\noutput:%s\nclean data:%s\n", input, output, cleanDate));

        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(output);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);//如果输出路径存在，就将其删除
        }

       // Configuration conf = new Configuration();
       // conf.set("mapreduce.job.queuename", "dmp");
        conf.set("mapreduce.job.name", "idmappingStep1");
        conf.set("orc.mapred.map.output.key.schema","struct<global_id:string,ids:map<string,map<string,struct<src:string,datetime:int,model:string>>>>");
        conf.set("orc.mapred.map.output.value.schema","struct<global_id:string,ids:map<string,map<string,struct<src:string,datetime:int,model:string>>>>");
        conf.set("orc.mapred.output.schema", "struct<global_id:string,ids:map<string,map<string,struct<src:string,datetime:int,model:string>>>>");
        conf.set("idmapping.clean.date", cleanDate);
        conf.setLong("mapreduce.map.memory.mb",9216);
        conf.setLong("mapreduce.reduce.memory.mb",9216);
        conf.set("mapreduce.map.java.opts","-Xmx9216m");
        conf.set("mapreduce.reduce.java.opts","-Xmx9216m");

        // 删除反作弊列表目录的数据
        String antiPathString = conf.get("id.blacklist.hdfs.path","hdfs://ns-hf/project/idmapping/idmapping/blacklist/");
        Path antiPath = new Path(antiPathString);
        if (!fs.exists(antiPath)) {
            fs.mkdirs(antiPath);
        }
        FileStatus[] fss = fs.listStatus(antiPath);
        for (FileStatus fstatus : fss) {
            fs.deleteOnExit(fstatus.getPath());
        }

        Job job = Job.getInstance(conf);
        // 输入输出路径
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setJarByClass(IDMappingStep1.class);
        job.setInputFormatClass(OrcInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrcValue.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(OrcStruct.class);
        job.setOutputFormatClass(OrcOutputFormat.class);
        job.setMapperClass(Step1M.class);
        job.setReducerClass(Step1R.class);
        job.setNumReduceTasks(500);
//        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
        return 0;
    }

    public void setConf(Configuration configuration) {
        conf = configuration;
    }

    public Configuration getConf() {
        return conf;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new IDMappingStep1(), args);
        System.exit(res);
    }
}
