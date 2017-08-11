package com.iflytek.cp.dmp.idmapping.hbase;

import com.iflytek.cp.dmp.idmapping.struct.IDs;
import com.iflytek.cp.dmp.idmapping.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;

import java.io.IOException;

import static com.iflytek.cp.dmp.idmapping.hbase.RegionSpliter.getHexSplits;

public class LoadIDs2HBase implements Tool {

    private Configuration conf = new Configuration();

    /**
    *将一条IDs记录转为json字符串作为value，写入HBase中
    * 采用批量bulkload方式
     */
    public static class IDs2HBaseMapper extends Mapper<NullWritable, OrcStruct, ImmutableBytesWritable, Put> {
        byte[] family = Bytes.toBytes("ids");
        byte[] qualifier = Bytes.toBytes("value");

        @Override
        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
            IDs ids = new IDs();
            ids.fromOrcStruct(value,false);
            String globalId = ids.globalID;
            String idsJson  = ids.toString();
            byte[] rowKey = Bytes.toBytes(globalId);
            ImmutableBytesWritable rowKeyWritable=new ImmutableBytesWritable(rowKey);
            byte[] hValue = Bytes.toBytes(idsJson);
            Put put=new Put(rowKey);
            put.addColumn(family, qualifier, hValue);
            context.write(rowKeyWritable,put);
        }
    }

    public int run(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];
        String tableName = args[2];
        System.out.println(String.format("bulkload ids to hbase \n input:%s\noutput:%s\ntableName:%s\n", inputPath, outputPath, tableName));
        conf = HBaseUtil.conf;
        byte[][] splits = getHexSplits("0000","FFFF",800);
        HBaseUtil.createIDsTable(tableName,splits);
        Job job = new Job(conf,"ids2hbase");
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(IDs2HBaseMapper.class);
        job.setInputFormatClass(OrcInputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);

        job.setNumReduceTasks(0);
        job.setJarByClass(LoadIDs2HBase.class);
        HTable table = new HTable(conf, tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, table);
        int ret = (job.waitForCompletion(true) ? 0 : 1);
        // job 跑完之后进行bulkload，将job输出目录权限设为777，bulkload完该目录为空
        if(ret == 0) {
            System.out.println("chmod :" + outputPath);
            Runtime.getRuntime().exec("hadoop fs -chmod -R 777 " + outputPath);
            System.out.println("start bulk load ...");
            LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(conf);
            loadFfiles.doBulkLoad(new Path(outputPath), table);//导入数据
            System.out.println("Bulk Load Completed ...");
            // zoo.setData("/idmapping/active_ids2hbase",tableName);
        }
        return ret;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new LoadIDs2HBase(), args);
        System.exit(res);
    }
}
