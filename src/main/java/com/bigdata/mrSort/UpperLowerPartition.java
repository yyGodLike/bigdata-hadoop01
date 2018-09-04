package com.bigdata.mrSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
/**
 * map阶段进行分区
 * @author liuhongyang
 *
 */
public class UpperLowerPartition extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numPartitions) {
		// 0代表第一个reduce，1代表第二个reduce，0表示大写，1表示小写
		String str = key.toString();
		
		
		
		// 使用正则表达式进行匹配，如果大写返回0，否则返回1
		if (str.substring(0, 1).matches("A-Z")) {
			return 0;
		}
		return 1;
	}

}
