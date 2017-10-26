package com.zhiyou100.bd17;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsUtils{
	
	private static final Configuration CONF
						= new Configuration();
	private static FileSystem hdfs;
	
	static{
		try {
			hdfs = FileSystem.get(CONF);
		} catch (IOException e) {
			System.out.println("无法连接hdfs, 请检查配置.");
			e.printStackTrace();
		}
	}
	
	//write
	public static void createFile(
			String fileName, 
			String content) 
					throws Exception{
		Path path = 
				new Path(fileName);
		if(hdfs.exists(path)){
			System.out.println("文件已经存在");
		}else{
			FSDataOutputStream outputStream =
					hdfs.create(path);
			outputStream.writeUTF(content);
			outputStream.flush();
			outputStream.close();
		}
	}
	
	
	//read
	public static void readFile(String fileName) 
			throws IOException{
		Path path = 
				new Path(fileName);
		if(!hdfs.exists(path) || hdfs.isDirectory(path)){
			System.out.println("给定路径"
					+fileName+
					"不存在, 或者不是一个文件");
		}else{
			FSDataInputStream inputStream = 
					hdfs.open(path);
			String content = inputStream.readUTF();
			System.out.println(content);
		}
		
	}
	
	
	//delete
	public static void deleteFile(
			String fileName) 
					throws IOException{
		Path path = 
				new Path(fileName);
		if(!hdfs.exists(path)){
			System.out.println(
					"给定文件路径:"
					+fileName+
					"不存在");
		}else{
			hdfs.delete(path, true);
		}
	}
	
	
	//upload
	public static void uploadFile(
			String windowsFileName
			, String hdfsPathName) 
					throws IOException{
		Path src = 
				new Path(windowsFileName);
		Path dst = 
				new Path(hdfsPathName);
		
		hdfs.copyFromLocalFile(
				src, dst);
	}
	
	
	//download
	public static void downloadFile(
			String hdfsFileName, 
			String windowsPathName) 
					throws IOException{
		Path src = 
				new Path(hdfsFileName);
		//下载到windows上
		Path dst = 
				new Path(windowsPathName);
		hdfs.copyToLocalFile(src, dst);
	}
	
	
	//get file status
	public static void getFileStatus(
			String fileName) 
			throws 
			FileNotFoundException, 
			IOException{
		
		Path path = new Path(fileName);
		FileStatus[] fileStatus = 
				hdfs.listStatus(path);
		for(FileStatus fs : fileStatus){
			if(fs.isFile()){
				System.out.println("文件:\t"+fs);
			}
			if(fs.isDirectory()){
				System.out.println("文件目录:\t"+fs);
			}
		}
	}
	
	
	
	public static void main(String[] args) 
			throws Exception{
		
		String content = 
				"郭仁杰";
		String fileName = 
				"/user/output/HdfsUtils/createFile.txt";
//		createFile(fileName, content);
		
//		readFile("/user/output/HdfsUtils/createFile.txt");
//		deleteFile(fileName);
//		uploadFile(
//				"D:\\test\\三国演义原著.txt"
//				, "/user/output/HdfsUtils/"
//						+ "uploadFile/三国演义原著1.txt");
//		downloadFile(
//				"/user/output/HdfsUtils/uploadFile/三国演义原著1.txt", 
//				"D:\\test\\三国演义原著1.txt");
		getFileStatus(
				"/user/output/HdfsUtils");
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}