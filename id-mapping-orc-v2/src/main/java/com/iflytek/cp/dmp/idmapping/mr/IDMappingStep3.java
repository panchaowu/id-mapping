package com.iflytek.cp.dmp.idmapping.mr;

import com.iflytek.cp.dmp.idmapping.struct.IDs;
import com.iflytek.cp.dmp.idmapping.util.IDMappingUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Created by admin on 2017/7/19.
 */
public class IDMappingStep3 implements Tool {
    Configuration conf = new Configuration();

    /**
     * idmapping step III map class
     */
    public static class Step3M extends Mapper<NullWritable, OrcStruct, Text, OrcValue> {

        private OrcValue orcValue;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            orcValue = new OrcValue();
        }

        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
            orcValue.value = value;
            IDs ids = new IDs();
            ids.fromOrcStruct(value, false);
            // TreeSet可保证有序
            TreeSet<String> idSet = new TreeSet<String>();
            idSet.addAll(IDMappingUtil.getAllIdSet(ids));
            StringBuffer sb = new StringBuffer();
            sb.append(idSet);
            context.write(new Text(sb.toString()), orcValue);
        }
    }

    /* idmapping step III reduce class
    * */
    public static class Step3R extends Reducer<Text, OrcValue, NullWritable, OrcStruct> {

        protected void reduce(Text key, Iterable<OrcValue> values, Context context) throws IOException, InterruptedException {
            // 转化为IDs结构，方便处理
            List<IDs> idss = new ArrayList<IDs>();
            for (OrcValue orcValue: values) {
                IDs ids = new IDs();
                OrcStruct orcStruct = (OrcStruct) orcValue.value;
                idss.add(ids.fromOrcStruct(orcStruct, false));
            }
            IDs newIDs = new IDs();
            // key相同，但其他字段不同
            for (IDs ids : idss) {
                IDMappingUtil.convergeID(newIDs, ids, false);
            }
            // global_id 为md5的大写
            newIDs.globalID = MD5Hash.getMD5AsHex(Bytes.toBytes(key.toString())).toUpperCase();
            context.write(NullWritable.get(), newIDs.toOrcStruct());
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Arguments error, [input] [output]");
            System.exit(-1);
        }

        String input = args[0];
        String output = args[1];
        System.out.println(String.format("idmapping step 3\n input:%s\noutput:%s\n", input, output));

        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(output);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);//如果输出路径存在，就将其删除
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.job.queuename", "dmp");
        conf.set("mapreduce.job.name", "idmappingStep3");
        conf.set("orc.mapred.map.output.key.schema","struct<global_id:string,ids:map<string,map<string,struct<src:string,datetime:int,model:string>>>>");
        conf.set("orc.mapred.map.output.value.schema","struct<global_id:string,ids:map<string,map<string,struct<src:string,datetime:int,model:string>>>>");
        conf.set("orc.mapred.output.schema", "struct<global_id:string,ids:map<string,map<string,struct<src:string,datetime:int,model:string>>>>");

        Job job = Job.getInstance(conf);
        // 输入输出路径
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setJarByClass(IDMappingStep3.class);
        job.setInputFormatClass(OrcInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrcValue.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(OrcStruct.class);
        job.setOutputFormatClass(OrcOutputFormat.class);

        job.setJarByClass(IDMappingStep3.class);
        job.setMapperClass(Step3M.class);
        job.setReducerClass(Step3R.class);
//        job.setNumReduceTasks(300);
        job.setNumReduceTasks(1);
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
        int res = ToolRunner.run(new IDMappingStep3(), args);
        System.exit(res);
    }
}
