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

public class HomeWorkTest {
	public static final Configuration CONF = new Configuration();
	public static FileSystem hdfs;

	static {
		try {
			hdfs = FileSystem.get(CONF);
		} catch (Exception e) {
			System.out.println("无法连接hdfs, 请检查配置.");
		}
	}

	// byte upload
	public static void uploadFile(String src, String dst) throws IOException {
		File file = new File(src);
		Path dstPath = new Path(dst);

		if (hdfs.exists(dstPath)) {
			System.out.println("给定路径" + dstPath + "不存在.");
			return;
		}

		if (!file.exists() || file.isDirectory()) {
			System.out.println("给定路径" + src + "不存在, 或者是一个目录");
		} else {
			// FSDataOutputStream, 创建文件在HDFS
			FSDataOutputStream outputStream = hdfs.create(dstPath, false);
			// 读取文件
			FileInputStream fis = new FileInputStream(src);

			byte[] b = new byte[2];

			int len = 0;
			while ((len = fis.read(b)) != -1) {
				// System.out.println(new String(b,0,len));
				outputStream.write(b);
				outputStream.flush();
			}

			fis.close();
			outputStream.close();
		}
	}

	// byte download, src为Hadoop路径, dst为Windows路径
	public static void downloadFile(String src, String dst) throws IOException {
		Path path = new Path(src);

		if (!hdfs.exists(path) || hdfs.isDirectory(path)) {
			System.out.println("给定路径" + src + "不存在, 或者是一个目录");
		} else {
			FileOutputStream fos = new FileOutputStream(new File(dst));
			FSDataInputStream inputStream = hdfs.open(path);

			byte[] b = new byte[2];
			int len = 0;
			while ((len = inputStream.read(b)) != -1) {
				System.out.println(new String(b, 0, len));
				fos.write(b, 0, len);
				fos.flush();
			}
			fos.close();
			inputStream.close();
		}
	}

	
	
	
	
	
	
	
	
	
	
	//查第一层表面
	private static File[] findFiles(String localSrc) {
		// 读取本地文件夹, localSrc
		File file = new File(localSrc);
		//文件进行过滤
		File[] listFiles = file.listFiles(new FileFilter() {

			//文件过滤
			@Override
			public boolean accept(File file) {
				return true;
			}
		});
		return listFiles;
	}

	private static void showFiles(String localSrc) throws Exception{
		File[] listFiles = findFiles(localSrc);
		
		//当前文件夹
		String currentFileName = localSrc;
		String toFileName = null;
		
		for(File f : listFiles){
			
			currentFileName = f.getAbsolutePath();
			System.out.println(currentFileName);
			
			toFileName = getFileName(currentFileName);
			System.out.println(toFileName);
			
			if(f.isDirectory()){
				new File(toFileName).mkdirs();
			}
			
		
			if(!f.isDirectory()){
				//写入程序FileIntputStream, 输出程序FileOutStream
				FileInputStream fis = new FileInputStream(new File(f.getAbsolutePath()));
				FileOutputStream fos = new FileOutputStream(new File(toFileName));
				
				byte[] b = new byte[2];
				int len = 0;
				while((len = fis.read(b)) != -1){
					fos.write(b, 0, len);
					fos.flush();
				}
				
				fos.close();
				fis.close();
			}
			
			
			
			if(f.isDirectory()){
				showFiles(f.getAbsolutePath());
			}
		}
	}
	
	public static String getFileName(String currentFileName){
		String[] cArr = currentFileName.split("\\\\");
		//System.out.println(Arrays.toString(cArr));
		
		String toFileName = "D:\\develop\\developtool\\Notepad1";
		String[] tArr = toFileName.split("\\\\");
		//System.out.println(Arrays.toString(tArr));
		
		System.arraycopy(tArr, 0, cArr, 0, tArr.length);
		//System.out.println(Arrays.toString(cArr));
		
		String string = Arrays.toString(cArr).replaceAll(",", "\\\\").replaceAll("\\[", "").replaceAll("\\]", "").replaceAll(" ", "").trim();
		//System.out.println(string);
		
		return string;
	}
	
	
	/*
	 * 上传文件夹 
	 * 1. 读取到每个文件
	 */
	public static void uploadDirectory(String localSrc, String hdfsDst) {

	}

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		// uploadFile("D:\\三国演义原著.txt", "/dirFromJava/三国演义原著3.txt");
//		downloadFile("/dirFromJava/三国演义原著.txt", "D:\\三国演义原著1.txt");
		showFiles("D:\\develop\\developtool\\Notepad++");
	}
}
