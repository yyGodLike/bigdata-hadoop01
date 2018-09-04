package com.bigdata.mrSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
/**
 * 分区操作
 * 
 * @author liuhongyang
 *
 */
public class FirstPartitioner extends Partitioner<PairWritable, IntWritable> {

	/**
	 * HashPartitioner是处理Mapper任务输出的,getPartition()方法有三个形参,key,value分别是Mapper任务的输入值,
	 * numPartitions指的是设置的Reduce任务数量,默认值是1,那么任何整数与1相除的余数肯定是0,也就是说getPartition(...)
	 * 方法的返回值总是0,也就是Mapper任务的输出总是送给第一个reduce任务,最终只能输入到一个文件中。
	 * 据此分析如果想要最终输出到多个文件中,在Mapper任务中对数据应该划分到多个区中,那么我们只需要按照一定的规则让getPartition(…)
	 * 方法的返回值是0,1,2,3…即可
	 */
	@Override
	public int getPartition(PairWritable key, IntWritable value, int numPartitions) {
		
		//默认使用key的hash值与上int的最大值，避免出现数据溢出 的情况
		return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}
