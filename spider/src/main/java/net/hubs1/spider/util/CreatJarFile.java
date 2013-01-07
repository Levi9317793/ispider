package net.hubs1.spider.util;

import java.io.File;

import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.Copy;
import org.apache.tools.ant.taskdefs.Delete;
import org.apache.tools.ant.taskdefs.Jar;
import org.apache.tools.ant.taskdefs.Mkdir;
import org.apache.tools.ant.types.FileSet;

public class CreatJarFile {
	private String baseDir=null;
	private String jarName=null;
	private String format=null;
	public CreatJarFile(String baseDir,String jarName,String format){
		this.baseDir=baseDir;
		this.jarName=jarName;
		this.format=format;
	}
	
	public void create(){
		Project p=new Project();
		p.setBaseDir(new File(baseDir));
		p.setName("spider");
		
		Mkdir mkdir=new Mkdir();
		mkdir.setDir(new File(baseDir+File.separator+"tmp"));
		
		FileSet fs=new FileSet();
		fs.setDir(new File(baseDir+File.separator+"bin"));
		
		Copy task=new Copy();
		task.setProject(p);
		task.addFileset(fs);
		task.setTodir(new File(baseDir+File.separator+"tmp"));
		task.execute();
		
		FileSet jsoup=new FileSet();
		jsoup.setDir(new File(baseDir+File.separator+"lib"));
		
		task=new Copy();
		task.setProject(p);
		task.addFileset(jsoup);
		task.setTodir(new File(baseDir+File.separator+"tmp"+File.separator+"lib"));
		task.execute();
		
		task=new Copy();
		task.setProject(p);
		task.setFile(new File(format));
		
		task.setTofile(new File(baseDir+File.separator+"tmp/net/hubs1/spider/hbase/mapredure/default.txt"));
		task.execute();
		
		Jar jar=new Jar();
		jar.setBasedir(new File(baseDir+File.separator+"tmp"));
		jar.setDestFile(new File(jarName));
		jar.setLevel(9);
		jar.setProject(p);
		
		jar.execute();
		
		Delete d=new Delete();
		d.setDir(new File(baseDir+File.separator+"tmp"));
		d.execute();
	}
	
	public void create2(){
		Project p=new Project();
		p.setBaseDir(new File(baseDir));
		p.setName("spider");
		
		Mkdir mkdir=new Mkdir();
		mkdir.setDir(new File(baseDir+File.separator+"tmp"));
		
		FileSet fs=new FileSet();
		fs.setDir(new File(baseDir+File.separator+"bin"));
		
		Copy task=new Copy();
		task.setProject(p);
		task.addFileset(fs);
		task.setTodir(new File(baseDir+File.separator+"tmp"));
		task.execute();
		
		FileSet lib=new FileSet();
		lib.setDir(new File(baseDir+File.separator+"lib"));
		
		task=new Copy();
		task.setProject(p);
		task.addFileset(lib);
		task.setTodir(new File(baseDir+File.separator+"tmp"+File.separator+"lib"));
		task.execute();
		
		task=new Copy();
		task.setProject(p);
		task.setFile(new File(format));
		
		task.setTofile(new File(baseDir+File.separator+"tmp/net/hubs1/spider/hbase/mapredure/default.txt"));
		task.execute();
		
		Jar jar=new Jar();
		jar.setBasedir(new File(baseDir+File.separator+"tmp"));
		jar.setDestFile(new File(jarName));
		jar.setLevel(9);
		jar.setProject(p);
		jar.setManifest(new File("D:\\workbench\\helios\\SpiderTask\\resource\\MANIFEST.MF"));
		
		jar.execute();
		
		Delete d=new Delete();
		d.setDir(new File(baseDir+File.separator+"tmp"));
		d.execute();
	}
	public static void main(String[] args) {
		//CreatJarFile cjf=new CreatJarFile("D:\\workbench\\helios\\SpiderTask", "D:\\extractHotelCN1.jar","D:\\workbench\\helios\\SpiderTask\\formats\\extract_agoda_cn_hotelinfo.txt");
		CreatJarFile cjf=new CreatJarFile(
				"D:\\workbench\\helios\\SpiderTask", 
				"D:\\sendmessage5.jar",
				"D:\\workbench\\helios\\SpiderTask\\formats\\extract_agoda_cn_hotelinfo.txt"
			);
		cjf.create();
//		cjf.create2();
	}
}
