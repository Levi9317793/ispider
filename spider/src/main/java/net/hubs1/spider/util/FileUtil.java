package net.hubs1.spider.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
//import java.util.zip.ZipEntry;
//import java.util.zip.ZipOutputStream;
//import org.apache.tools.ant.*;;
import org.apache.tools.zip.ZipEntry;
import org.apache.tools.zip.ZipOutputStream;
public class FileUtil {
	
	/**
	 * @param file
	 * @param zipfile 指定的压缩文件的存放位置及名称，如未知道，方法自动在需要压缩的文件当前目录下保存压缩文件，文件名为现有文件名后加后缀.zip
	 * @param ziplevel 压缩级别，其值为0至9. 在指定的数值范围内，数值越大，表示压缩程度越高。
	 * @return
	 * @throws IOException
	 */
	public static File zipFile(File file,File zipfile,int ziplevel) throws IOException{
		FileInputStream fistream=new FileInputStream(file);
		FileChannel filec=fistream.getChannel();
		ByteBuffer inbuffer=ByteBuffer.allocate(20480);
		
		File outfile=zipfile==null?new File(file.getAbsolutePath()+".zip"):zipfile;
		ZipOutputStream zo=new ZipOutputStream(new FileOutputStream(outfile));
		if(ziplevel<0) ziplevel=0;
		if(ziplevel>9) ziplevel=9;
		
		zo.setLevel(ziplevel);
		//file.getName().format(l, format, args)
		//String filename=ostype.equals("windows")?new String(file.getName().getBytes(),"GBK"):file.getName();
		String filename=file.getName();
		zo.putNextEntry(new ZipEntry(filename)); 
		while(filec.read(inbuffer)!=-1){
			zo.write(inbuffer.array());
			inbuffer.clear();
		}
		fistream.close();
		
		zo.close();
		return outfile;
	}
	
	
	
	/**
	 * @param files
	 * @param zipfile 指定的压缩文件的存放位置及名称，必须指定，否则返回null.
	 * @param ziplevel 压缩级别，其值为0至9. 在指定的数值范围内，数值越大，表示压缩程度越高。
	 * @return
	 * @throws IOException
	 */
	public static void zipFiles(Collection<File> files,File zipfile,int ziplevel) throws IOException{
		if(files==null||files.isEmpty()||zipfile==null) return ;
		
		ZipOutputStream zo=new ZipOutputStream(new FileOutputStream(zipfile));
		if(ziplevel<0) ziplevel=0;
		if(ziplevel>9) ziplevel=9;
		zo.setLevel(ziplevel);
		
		for(File file:files){
			FileInputStream fistream=new FileInputStream(file);
			FileChannel filec=fistream.getChannel();
			ByteBuffer inbuffer=ByteBuffer.allocate(20480);
			zo.putNextEntry(new ZipEntry(file.getName())); 
			while(filec.read(inbuffer)!=-1){
				zo.write(inbuffer.array());
				inbuffer.clear();
			}
			fistream.close();
		}
		zo.close();
	}
	
	/**
	 * 拷贝文件
	 * @param in
	 * @param outr
	 * @throws IOException
	 */
	public static void copyFile(File in,File out) throws IOException{
		
		FileInputStream fin = new FileInputStream(in);
		FileOutputStream fout = new FileOutputStream(out);

		FileChannel fcin = fin.getChannel();
		FileChannel fcout = fout.getChannel();

		ByteBuffer buffer = ByteBuffer.allocate(1024);
		while (fcin.read(buffer)!=-1) {
			buffer.flip();
			fcout.write(buffer);
			buffer.clear();
		}
		fout.close();
		fin.close();
	}
	
	 public static void main(String[] args) throws IOException{
//		File input=new File("E:/每日预订离店及Noshow报表_2011-06-08.xls");
//		FileUtil.zipFile(input, null,1);
		 String rowkey="agoda.image.hotelimages.641.64165.64165_110324110759256_ST";
		 int first=rowkey.indexOf(".");
		 int second=rowkey.indexOf(".", first+1);
		 	String queueName="spider"+"."+rowkey.substring(0,second);
		 	System.out.println(queueName);
	}
}
