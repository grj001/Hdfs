package com.zhiyou100.bd17;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HomeWorkTest{
	
	public static final Configuration CONF = 
			new Configuration();
	public static FileSystem hdfs;
	
	static {
		try {
			hdfs = FileSystem.get(CONF);
		} catch (IOException e) {
			System.out.println("无法连接hdfs, 请检查配置");
			e.printStackTrace();
		}
	}
	
	//byte upload
	public static void uploadFile(String src, String dst) throws IOException{
		File file = 
				new File(src);
		Path dstPath =
				new Path(dst+"/"+file.getName());
		
		//要上传到hdfs上的目标文件夹
		if(!hdfs.exists(dstPath)){
			System.out.println(
					"给定路径"
					+dstPath+
					"不存在");
		}
		
		if(!file.exists() || file.isDirectory()){
			System.out.println("给定路径"+src+"不存在, 或者是一个目录");
		}else{
			// FSDataOutputStream, 在hdfs创建文件
			FSDataOutputStream outputStream = hdfs.create(dstPath, true);
			
			//读取文件
			FileInputStream fis = 
					new FileInputStream(file);
			
			byte[] b = 
					new byte[2];
			
			int len = 0;
			while(
					(len = fis.read(b)) != -1
					){
				outputStream.write(b, 0, len);
				//将缓冲区中的, 内容刷到系统磁盘中
				outputStream.flush();
			}
			fis.close();
			outputStream.close();
		}
	}
	
	
	// byte download, src为hadoop路径, dst为windows路径
	public static void downloadFile(
			String src
			, String dst) 
					throws IOException{
		
		Path path = new Path(src);
		
		if(!hdfs.exists(path) || hdfs.isDirectory(path)){
			System.out.println("给定路径"+src+"不存在, 或者是一个目录");
		}else{
			//从hdfs上读取文件, 输入到内存中
			FSDataInputStream inputStream = 
					hdfs.open(path);
			//从内存中输出, 写到windows文件中
			FileOutputStream fos = 
					new FileOutputStream(new File(dst+"/"+path.getName()));
			
			byte[] b =
					new byte[2];
			int len = 0;
			while(
					(len = inputStream.read(b)) != -1
					){
				fos.write(b, 0, len);
				fos.flush();
			}
			fos.close();
			inputStream.close();
		}
		
	}
	
	
	
	
	//查看文件夹下第一层子目录
	public static File[] findFiles(String localSrc){
		//创建本地文件, localSrc
		File file = 
				new File(localSrc);
		
		//文件进行过滤
		File[] listFiles = file.listFiles(new FileFilter() {
			
			@Override
			public boolean accept(File pathname) {
				return true;
			}
		});
		return listFiles;
	}
	
	
	public static void copyFiles(
			String localSrc
			, String toFileDirectory) 
			throws IOException{
		
		File[] listFiles = findFiles(localSrc);
		
		//当前文件夹
		String currentFileName = "";
		//要复制的目的地
		String toFileName = toFileDirectory;
		
		if(!new File(toFileName).exists()){
			new File(toFileName).mkdirs();
		}else{
			new File(toFileName).delete();
			new File(toFileName).mkdirs();
		}
		
		
		for(File f : listFiles){
			
			currentFileName = f.getAbsolutePath();
			toFileName = getToFileName(currentFileName, toFileDirectory);
			
			System.out.println(currentFileName);
			System.out.println(toFileName);
			
			if(f.isDirectory()){
				new File(toFileName).mkdirs();
			}
			
			if(!f.isDirectory()){
				//写入程序FileInputStream, 输出程序FileOutputStream
				FileInputStream fis = 
						new FileInputStream(
								new File(currentFileName));
				FileOutputStream fos = 
						new FileOutputStream(
								new File(toFileName)
								);
				
				byte[] b = 
						new byte[2];
				int len = 0;
				while(
						(len = fis.read(b)) != -1
						){
					fos.write(b, 0, len);
					fos.flush();
				}
				fis.close();
				fos.close();
			}
			
			if(f.isDirectory()){
				copyFiles(currentFileName, toFileName);
			}
			
		}
	}
	
	
	public static String getToFileName(String currentFileName, String toFileDirectory){
		String[] cArr = currentFileName.split("\\\\");
		
		String[] tArr = 
				toFileDirectory.split("\\\\");
		
		System.arraycopy(
				tArr, 0 //源数组
				, cArr, 0 //要copy到的目标数组
				, tArr.length); //要copy源数组的长度
		
		String toFileName = 
				Arrays.toString(cArr)
				.replaceAll("[,]", "\\\\")
				.replaceAll("[\\[\\]\\s]", "")
				.trim();
		
		return toFileName;
	}
	
	
	
	public static void main(String[] args) 
			throws IOException{
		
//		uploadFile(
//				"D:\\test\\三国演义原著.txt"
//				, "/user/output/HomeWorkTest");
		
//		downloadFile("/user/user_info.txt", "D:\\test\\");
		
		
		copyFiles("D:\\test\\三国演义分章节", "D:\\test\\三国演义分章节1");
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}