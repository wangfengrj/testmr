package org.inspur.wf;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsTest {
	private static Configuration conf = new Configuration();

	// 创建目录
	public static void mkdir(String path) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(path);
		boolean isok = fs.mkdirs(srcPath);
		if (isok) {
			System.out.println("create dir ok!");
		} else {
			System.out.println("create dir failure");
		}
		fs.close();
	}
	
	public static void main(String[] args) throws IOException {
		System.setProperty("HADOOP_USER_NAME", "admin");
		conf.set("fs.default.name", "hdfs://192.168.169.130:8020");
		Date dt = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		String today = sdf.format(dt);
		// 新建目录
		mkdir("/"+today);	
	}
	

}