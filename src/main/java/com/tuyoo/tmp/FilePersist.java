package com.tuyoo.tmp;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.log4j.Logger;

public class FilePersist {
	
	private Logger logger = Logger.getLogger(FilePersist.class);
	private String fileName;
	private String parentPath;
	private File file;
	private FileWriter fw;
	
	public FilePersist (String parentPath, String fileName){
		this.parentPath = parentPath;
		this.fileName = fileName;
		init();
	}
	
	public FilePersist(String fileName) {
		this("",fileName);
	}
	
	private void init(){
		if(this.parentPath != null && !"".equals(this.parentPath)){
			File dir = new File(parentPath);
			if(!dir.exists()){
				logger.info("路径 ["+parentPath+"] 不存在，创建。。。");
				dir.mkdirs();
			}
			if(!parentPath.endsWith("/")){
				this.parentPath+="/";
			}
		}
		this.file = new File(this.parentPath+this.fileName);
		
		logger.info("初始化文件=====》"+file.getPath());
		try {
			this.fw = new FileWriter(this.file);
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	public void writeLine(String line){
		try {
			fw.write(line+"\n");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	public void close(){
		try {
			logger.info("文件《"+file.getAbsolutePath()+"》写入完毕， 关闭流。。。");
			this.fw.close();
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
}
