package com.zhiyou100.bd17;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsTest {

	//fileSystem, 
	public static FileSystem getFileSystem() throws IOException{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		return hdfs;
	}
	
	//mkdir
	public static void mkDir(FileSystem hdfs, String dirName) throws IOException{
		Path path = new Path(dirName);
		if(hdfs.mkdirs(path)){
			System.out.println("创建目录:"+dirName+"成功");
		}else{
			System.out.println("创建目录:"+dirName+"失败");
		}
	}
	
	public static void main(String[] args) throws IOException {
		FileSystem hdfs = getFileSystem();
		mkDir(hdfs, "/dirFromJava");
	}
}
