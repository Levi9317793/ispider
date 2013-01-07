package net.hubs1.spider.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.tools.zip.ZipEntry;
import org.apache.tools.zip.ZipOutputStream;

public class CreatZipFile {
//	private File outFile=null;
	private ZipOutputStream zo=null;
	private String baseFilePath=null;
	private String dirFilter=null;
	private String fileFilter=null;
	
	public CreatZipFile(File outFile,Integer zipLevel){
		try {
			if(!outFile.exists()){
				outFile.createNewFile();
			}
			zo=new ZipOutputStream(new FileOutputStream(outFile));
			
			if(zipLevel!=null){
				if(zipLevel<0) zipLevel=0;
				if(zipLevel>9) zipLevel=9;
				
				zo.setLevel(zipLevel);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void setBaseFilePath(String path){
		this.baseFilePath=path.toLowerCase();
	}
	
	
	/**
	 * 将某一个文件添加到压缩文件的一个指定目录下。 
	 * 目录不存在，则创建。
	 * @param file
	 * @param direct
	 * @throws IOException
	 */
	public void addFile(File file,String newName,String zipDir) throws IOException{
		if(fileFilter!=null&&file.getName().contains(fileFilter)) return;
			
		FileInputStream fistream=new FileInputStream(file);
		FileChannel filec=fistream.getChannel();
		ByteBuffer inbuffer=ByteBuffer.allocate(20480);
		
		if(baseFilePath!=null&&zipDir==null){
			zipDir=file.getParent().toLowerCase();
			zipDir=zipDir.replace(baseFilePath,"");
			if(zipDir.startsWith(File.separator)) zipDir=zipDir.substring(1);
			if(zipDir.length()==0) zipDir=null;
		}
		
		newName=newName!=null?newName:file.getName();
		newName=zipDir!=null?zipDir+"/"+newName:newName;
		
		ZipEntry ze=new ZipEntry(newName);
		//CRC32 cal = new CRC32();
		//ze.setSize (inbuffer.);
       // ze.setMethod (ZipEntry.STORED);
//		ze.setUnixMode(3);
		//ze.setMethod (ZipEntry.STORED);
        //ze.setUnixMode(ze.getUnixMode());
		
		zo.putNextEntry(ze); 
		while(filec.read(inbuffer)!=-1){
			zo.write(inbuffer.array());
			inbuffer.clear();
		}
		zo.closeEntry();
		fistream.close();
	}
	
	
	public void addFile(File file) throws IOException{
		addFile(file,null,null);
	}
	
	/**
	 * 将某一个文件添加到压缩文件的一个指定目录下。 
	 * 目录不存在，则创建。
	 * @param file
	 * @param direct
	 * @throws IOException
	 */
	public void addFile(File file,String zipDir) throws IOException{
		addFile(file,null,zipDir);
	}
	
	public void finish() throws IOException{
		zo.close();
	}
	
	public void setDirFilter(String dirFilter) {
		this.dirFilter = dirFilter;
	}


	public void setFileFilter(String fileFilter) {
		this.fileFilter = fileFilter;
	}
	
	/**
	 * 添加某一目录下的所有文件添加到压缩文件的一个指定目录下。 
	 * 目录不存在，则创建。
	 * @param file
	 * @param direct
	 * @throws IOException
	 */
	public void addAllFile(File dir,String zipDir,boolean isFind) throws IOException{
		File[] fileList=dir.listFiles();
		if(fileList==null||fileList.length==0) return;
		for(File file:fileList){
			if(file.isFile()){
				addFile(file,null,zipDir);
			}else{
				if(dirFilter!=null&&!file.getName().contains(dirFilter))
					addAllFile(file, zipDir, isFind);
			}
		}
	}
	
	public static void main(String[] args) throws IOException {
//		CreatJarFile cjf=new CreatJarFile(new File("D:/extractHotelCN.jar"),null);
//		cjf.setBaseFilePath("D:\\workbench\\helios\\SpiderTask\\bin");
//		cjf.setDirFilter("svn");
//		
//		cjf.addAllFile(new File("D:/workbench/helios/SpiderTask/bin"),null,true);
//		cjf.addFile(new File("D:/workbench/helios/SpiderTask/formats/extract_agoda_cn_hotelinfo.txt"),"default.txt","net/hubs1/spider/hbase/mapredure");
//		cjf.addFile(new File("D:/workbench/helios/SpiderTask/lib/jsoup-1.6.1.jar"), "lib");
//		cjf.addFile(new File("D:/workbench/helios/SpiderTask/formats/MANIFEST.MF"), "META-INF");
//		cjf.finish();
//		Project p=new Project();
//		p.setBaseDir(new File("D:\\workbench\\helios\\SpiderTask"));
//		p.setName("spider");
//		
//		Mkdir mkdir=new Mkdir();
//		mkdir.setDir(new File("D:\\workbench\\helios\\SpiderTask\\tmp"));
//		
//		
//		FileSet fs=new FileSet();
//		fs.setDir(new File("D:\\workbench\\helios\\SpiderTask\\bin"));
//		
//		
//		
//		Copy task=new Copy();
//		task.setProject(p);
//		task.addFileset(fs);
//		task.setTodir(new File("D:\\workbench\\helios\\SpiderTask\\tmp"));
//		task.execute();
//		
//		FileSet jsoup=new FileSet();
//		FilenameSelector fns=new FilenameSelector();
//		fns.setName("jsoup-1.6.1.jar");
//		jsoup.addFilename(fns);
//		jsoup.setDir(new File("D:\\workbench\\helios\\SpiderTask\\lib"));
//		
//		task=new Copy();
//		task.setProject(p);
//		task.addFileset(jsoup);
//		task.setTodir(new File("D:\\workbench\\helios\\SpiderTask\\tmp\\lib"));
//		task.execute();
//		
//		
//		task=new Copy();
//		task.setProject(p);
//		task.setFile(new File("D:\\workbench\\helios\\SpiderTask\\formats\\extract_agoda_cn_hotelinfo.txt"));
//		task.setTofile(new File("D:\\workbench\\helios\\SpiderTask\\tmp\\net\\hubs1\\spider\\hbase\\mapredure\\default.txt"));
//		task.execute();
//		
//		
//		
//		Jar jar=new Jar();
//		jar.setBasedir(new File("D:\\workbench\\helios\\SpiderTask\\tmp"));
//		jar.setDestFile(new File("D:\\extractHotelCN1.jar"));
//		jar.setLevel(9);
//		jar.setProject(p);
//		
//		jar.execute();
//		
//		Delete d=new Delete();
//		d.setDir(new File("D:\\workbench\\helios\\SpiderTask\\tmp"));
//		d.execute();
		
		
	}

	
}
