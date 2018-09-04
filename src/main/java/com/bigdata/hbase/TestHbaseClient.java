package com.bigdata.hbase;

import java.io.InterruptedIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("all")
public class TestHbaseClient {
	
	/**
	 * 获取TABLE
	 * @param name
	 * @return
	 * @throws Exception
	 */
	private static HTable getTable(String name) throws Exception{
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, name);
		return table;
	}
	
	/**
	 * get data
	 * @param table
	 * @throws Exception
	 */
	private static void getData(HTable table) throws Exception{
		//get rowkey
		Get get = new Get(Bytes.toBytes("20170521_10001"));
		//查询rowkey下面的f1列簇
		get.addFamily(Bytes.toBytes("f1"));
		//table与get绑定
		Result result = table.get(get);
		for(Cell cell : result.rawCells()){
			System.out.print("列簇:"+Bytes.toString(CellUtil.cloneFamily(cell))+"\t");
			System.out.print("rowkey:"+Bytes.toString(CellUtil.cloneRow(cell))+"\t");
			System.out.print("列名:"+Bytes.toString(CellUtil.cloneQualifier(cell))+"\t");
			System.out.print("值:"+Bytes.toString(CellUtil.cloneValue(cell)));
			System.out.println();
		}
	}
	
	/**
	 * put Data
	 * @param table
	 * @throws Exception 
	 */
	private static void putData(HTable table) throws Exception{
		
		Put put = new Put(Bytes.toBytes("20170521_10001"));
		put.add(Bytes.toBytes("f1"),Bytes.toBytes("age"),Bytes.toBytes("20"));
		table.put(put);
		
		getData(table);
	}
	
	/**
	 * delete Data
	 * @param table
	 * @throws Exception
	 */
	public static void deleteData(HTable table) throws Exception {
		Delete del = new Delete(Bytes.toBytes("20170521_10001"));
		//deleteColumn是删除列簇中时间戳最新的一个版本
		//deleteColumns是删除列簇中的全部
		del.deleteColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"));
		table.delete(del);
		getData(table);
	}
	
	
	public static void scanData(HTable table) throws Exception {

		Scan scan = new Scan();

		// load the scan
		ResultScanner rsscan = table.getScanner(scan);

		for (Result rs : rsscan) {
			System.out.println(Bytes.toString(rs.getRow()));
			for (Cell cell : rs.rawCells()) {
				System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)) + "->"
						+ Bytes.toString(CellUtil.cloneQualifier(cell)) + "->"
						+ Bytes.toString(CellUtil.cloneValue(cell)) + "->" + cell.getTimestamp());
			}
			System.out.println("---------------------------------------------");
		}
	}
	
	
	
	public static void main(String[] args) throws Exception {
		System.out.println("12");
		HTable table = getTable("hbase1:t1");
		getData(table);
		putData(table);
	}
	
}
