package com.bigdata.hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * hbase集成MapReduce
 * @author liuhongyang
 *
 */
public class TestMrAndHbase extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf,"mr-Hbase");
		job.setJarByClass(TestMrAndHbase.class);
		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob(
				"stu_info",
				scan,
				MrHbaseMapper.class,
				ImmutableBytesWritable.class,
				Put.class,
				job);
		TableMapReduceUtil.initTableReducerJob(
				"t5",
				null,
				job);
		job.setNumReduceTasks(1);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) {
		
		Configuration conf = HBaseConfiguration.create();
		try {
			int status = ToolRunner.run(conf, new TestMrAndHbase(),args);
			System.exit(status);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
}
/**
 * map 读取数据
 * @author liuhongyang
 *
 */
class MrHbaseMapper extends TableMapper<ImmutableBytesWritable, Put>{

	@Override
	protected void map(ImmutableBytesWritable key, Result value,Context context)
			throws IOException, InterruptedException {
		//封装put
		Put put = new Put(key.get());
		for(Cell cell : value.rawCells()){
			//判断当前cell的列簇是否为info
			if("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
				//判断当前cell的列簇info的列是否为name
				if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
					put.add(cell);
				}
			}
		}
		context.write(key, put);
	}
	
	
	
}