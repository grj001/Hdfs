package com.zhiyou100.bd17;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsUtils {
	public static final Configuration CONF = new Configuration();
	public static FileSystem hdfs;
	
	static{
		try{
			hdfs = FileSystem.get(CONF);
		}catch (Exception e) {
			System.out.println("无法连接hdfs, 请检查配置.");
			e.printStackTrace();
		}
	}
	
	//wirte
	public static void createFile(String fileName, String content) throws IOException{
		Path path = new Path(fileName);
		if(hdfs.exists(path)){
			System.out.println("文件已存在");
		}else{
			FSDataOutputStream outputStream = hdfs.create(path);
			outputStream.writeUTF(content);
			outputStream.flush();
			outputStream.close();
		}
	}
	
	//read
	public static void readFile(String fileName) throws IOException{
		Path path = new Path(fileName);
		if(!hdfs.exists(path) || hdfs.isDirectory(path)){
			System.out.println("给定路径"+fileName+"不存在, 或者不是一个文件.");
		}else{
			FSDataInputStream inputStream = hdfs.open(path);
			String content = inputStream.readUTF();
			System.out.println(content);
		}
	}
	
	//delete
	public static void deleteFile(String fileName) throws IOException{
		Path path = new Path(fileName);
		if(!hdfs.exists(path)){
			System.out.println("给定的路径:"+fileName+"不存在");
		}else{
			hdfs.delete(path, true);
		}
	}
	
	//copyFromLocalFile
	public static void copyFile(String srcFileName, String fileName) throws IOException{
		Path srcPath = new Path(srcFileName);
		Path path = new Path(fileName);
		if(!(new File(srcFileName).exists())){
			System.out.println("给定路径"+srcFileName+"不存在");
		}else{
			hdfs.copyFromLocalFile(srcPath, path);
		}
	}
	
	//upload
	public static void uploadFile(String fileName,String hdfsPath) throws IOException{
		Path src = new Path(fileName);
		Path dst = new Path(hdfsPath);
		hdfs.copyFromLocalFile(src, dst);
	}
	
	//download
	public static void downloadFile(String hdfsPath,String localPath) throws IOException{
		Path src = new Path(hdfsPath);
		Path dst = new Path(localPath);
		hdfs.copyToLocalFile(src, dst);
	}
	
	//get status
	public static void getFileStatus(String fileName) throws FileNotFoundException, IOException{
		Path path = new Path(fileName);
		FileStatus[] fileStatus = hdfs.listStatus(path);
		for(FileStatus fs : fileStatus){
			
			//System.out.println("\t路径是:\t"+fs.getPath());
			//System.out.println(
			//		"\t是否是文件夹是:\t"+
			//		hdfs.isDirectory(new Path(fs.getPath().toString()))
			//		);
			/*if(!hdfs.isDirectory(new Path(fs.getPath().toString()))){
				System.out.println(fs);
			}
			if(hdfs.isDirectory(new Path(fs.getPath().toString()))){
				getFileStatus(fs.getPath().toString());
			}*/
			if(fs.isFile()){
				System.out.println(fs);
			}
			if(fs.isDirectory()){
				getFileStatus(fs.getPath().toString());
			}
		}
	}
	
	
	public static void main(String[] args) throws IOException {
		String content = "郭仁杰";
		String fileName = "/";
//		createFile(fileName, content);
//		readFile("/dirFromJava/三国演义原著.txt");
//		deleteFile(fileName);
//		uploadFile("D:\\三国演义原著.txt", "/dirFromJava/三国演义原著2.txt");
//		downloadFile("/dirFromJava/三国演义原著2.txt", "D:\\三国演义原著2.txt");
		getFileStatus(fileName);
	}
}
