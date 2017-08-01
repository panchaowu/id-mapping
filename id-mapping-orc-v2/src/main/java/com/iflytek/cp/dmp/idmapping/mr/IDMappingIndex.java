package com.iflytek.cp.dmp.idmapping.mr;

import com.iflytek.cp.dmp.idmapping.struct.IDs;
import com.iflytek.cp.dmp.idmapping.util.IDMappingUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;
import java.util.Set;

/**
 * Created by admin on 2017/7/19.
 * 用来生成Index
 */
public class IDMappingIndex implements Tool {
    private Configuration conf = new Configuration();

    /**
     * idmapping index map class
     */
    public static class Index3M extends Mapper<NullWritable, OrcStruct, Text, Text> {

        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
            IDs ids = new IDs();
            ids.fromOrcStruct(value, false);
            for (String type : IDMappingUtil.getValidTypeSet()) {
                Set<String> sets = ids.ids.get(type).keySet();
                for (String id : sets) {
                    context.write(new Text(type + "##" + id), new Text(ids.globalID));
                }
            }
        }
    }

    /*
     * idmapping index reduce class
     * */
    public static class Index3R extends Reducer<Text, Text, NullWritable, OrcStruct> {

        private MultipleOutputs<NullWritable, OrcStruct> mos;
        private TypeDescription indexSchema =
                TypeDescription.fromString("struct<id:string,global_id:string>");
        private OrcStruct orcStruct;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<NullWritable, OrcStruct>(context);
            orcStruct = (OrcStruct) OrcStruct.createValue(indexSchema);
        }

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 第一个是类型，第二个是global_id
            String[] strArray = key.toString().split("##");
            for (Text value : values) {
                orcStruct.setFieldValue("id", new Text(strArray[1]));
                orcStruct.setFieldValue("global_id", value);
                mos.write(strArray[0], NullWritable.get(), orcStruct, String.format("%s/part", strArray[0]));
                break;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Arguments error, [input] [output]");
            System.exit(-1);
        }

        String input = args[0];
        String output = args[1];
        System.out.println(String.format("idmapping index\n input:%s\noutput:%s\n", input, output));

        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(output);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);//如果输出路径存在，就将其删除
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.job.queuename", "dmp");
        conf.set("mapreduce.job.name", "idmapping index");
//        conf.set("orc.mapred.map.output.key.schema","struct<global_id:string,ids:map<string,map<string,struct<src:string,datetime:int,model:string>>>>");
//        conf.set("orc.mapred.map.output.value.schema","struct<id:string,global_id:string>");
        conf.set("orc.mapred.output.schema","struct<id:string,global_id:string>");

        Job job = Job.getInstance(conf);
        // 输入输出路径
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setJarByClass(IDMappingIndex.class);
        job.setInputFormatClass(OrcInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(OrcStruct.class);
        LazyOutputFormat.setOutputFormatClass(job,OrcOutputFormat.class);
        String[] outputPaths = conf.get("id.type.list","imei,mac,imsi,idfa,openudid,phone_number,android_id").split(",");
        for (String outPath : outputPaths) {
            MultipleOutputs.addNamedOutput(job, outPath.replace("_",""), OrcOutputFormat.class, NullWritable.class, OrcStruct.class);
        }
        job.setJarByClass(IDMappingIndex.class);
        job.setMapperClass(Index3M.class);
        job.setReducerClass(Index3R.class);
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
        int res = ToolRunner.run(new IDMappingIndex(), args);
        System.exit(res);
    }
}
