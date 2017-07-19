package com.iflytek.idmapping.hbase;

import com.iflytek.idmapping.Index.IndexOrcUtil;
import com.iflytek.idmapping.util.FileUtil;
import com.iflytek.idmapping.zookeeper.BaseZookeeper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by admin on 2017/6/23.
 */
public class LoadIndex2HBase implements Tool  {
    public static Configuration conf;

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static class ImportIndexMapper extends Mapper<NullWritable, Writable, ImmutableBytesWritable, Put> {
        byte[] family = Bytes.toBytes("global_id");
        byte[] qualifier = Bytes.toBytes("value");

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            IndexOrcUtil.setupIndexOrc();
        }

        @Override
        protected void map(NullWritable key, Writable value, Context context) throws IOException, InterruptedException {
            OrcStruct indexData = (OrcStruct) value;
            // （md5(toUpperCase).toUpper
            String id = MD5Hash.getMD5AsHex(IndexOrcUtil.getIndexId(indexData).toUpperCase().getBytes()).toUpperCase();
            String globalId = IndexOrcUtil.getIndexGlobalId(indexData);
            byte[] rowKey = Bytes.toBytes(id);
            ImmutableBytesWritable rowKeyWritable=new ImmutableBytesWritable(rowKey);
            byte[] hValue = Bytes.toBytes(globalId);
            Put put=new Put(rowKey);
            put.addColumn(family, qualifier, hValue);
            context.write(rowKeyWritable,put);
        }
    }

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


    public static String padLeft(String s, int length)
    {
        byte[] bs = new byte[length];
        byte[] ss = s.getBytes();
        Arrays.fill(bs, (byte) (48 & 0xff));
        System.arraycopy(ss, 0, bs,length - ss.length, ss.length);
        return new String(bs);
    }

    @Override
    public int run(String[] args) throws Exception {

        String inputPath = args[0];
        String outputPath = args[1];
        String tableName = args[2];
//        // zookeeper 操作
//        BaseZookeeper zoo = new BaseZookeeper();
//        zoo.connectZookeeper("10.10.12.82,10.10.12.83,10.10.12.84");
//        String inHBaseUse = zoo.getData("/idmapping/active_index2hbase");
//        String tableName = "";
//        if(inHBaseUse.equals("idmapping_index2hbase_1")) {
//            tableName = "idmapping_index2hbase_2";
//        } else if(inHBaseUse.equals("idmapping_index2hbase_2")) {
//            tableName = "idmapping_index2hbase_1";
//        } else {
//            System.out.println("inHBaseUse:" +  tableName  + "is invalid!");
//            zoo.closeConnection();
//            return 0;
//        }

        conf = HBaseUtil.conf;
        byte[][] splits = getHexSplits("0000","FFFF",800);
        HBaseUtil.createIndexTable(tableName,splits);
        Job job = new Job(conf,"index2hbase");
        List<String> inputDirs = FileUtil.getSubDirs(inputPath);
        for(String dir : inputDirs) {
            FileInputFormat.addInputPath(job,new Path(dir));
        }
        FileOutputFormat.setOutputPath(job,new Path(outputPath));

        job.setInputFormatClass(OrcNewInputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);

        job.setMapperClass(ImportIndexMapper.class);
        job.setNumReduceTasks(0);
        job.setJarByClass(LoadIndex2HBase.class);

        HTable table = new HTable(conf, tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, table);

        int ret = (job.waitForCompletion(true) ? 0 : 1);
        if(ret == 0) {
            System.out.println("chmod -R 777 " + outputPath);
            Runtime.getRuntime().exec("hadoop fs -chmod -R 777 " + outputPath);

            System.out.println("start bulk load index ...");
            LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(conf);
            loadFfiles.doBulkLoad(new Path(outputPath), table);//导入数据
            System.out.println("Bulk Load index Completed ...");
         //   zoo.setData("/idmapping/active_index2hbase",tableName);
        }
        //zoo.closeConnection();
        return ret;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new LoadIndex2HBase(), args);
        System.exit(res);
    }

}
