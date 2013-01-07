package net.hubs1.spider.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataUtil {
	private static long beforeTime=0;
	
	/**
	 * 从一个文件中读取记录，并将其放入一个列表中
	 * @param file 需要读取的文件
	 * @param spliteReg 每行文件的分隔符，如果有分隔符，则一行有可能对应列表中的多条记录
	 * @param deleteRepeat 是否删除返回列表中的重复记录
	 * @return
	 */
	public  static Collection<String> fileToCollection(File file,String spliteReg,boolean deleteRepeat) {
		Collection<String> kws=null;
		try{
			LineNumberReader line=new LineNumberReader(new FileReader(file));
					
			kws=deleteRepeat?new LinkedHashSet<String>():new ArrayList<String>();
			
			String kw=null;
			while((kw=line.readLine())!=null){
				if(kw==null||kw.trim().length()==0) continue;
				if(spliteReg==null){ 
					kws.add(kw);
				}else{
					String[] ks=kw.split(spliteReg);
					Collections.addAll(kws,ks);
				}
			}
			line.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		return kws;
	}
	
	
	/**
	 * 从一个文件中读取记录，并将其放入一个列表中
	 * @param file 需要读取的文件
	 * @param spliteReg 每行文件的分隔符，如果有分隔符，则一行有可能对应列表中的多条记录
	 * @param deleteRepeat 是否删除返回列表中的重复记录
	 * @return
	 */
	public  static Collection<String> readCommand(File file,String spliteReg,boolean deleteRepeat) {
		Collection<String> kws=null;
		try{
			LineNumberReader line=new LineNumberReader(new FileReader(file));
					
			kws=deleteRepeat?new LinkedHashSet<String>():new ArrayList<String>();
			
			String kw=null;
			while((kw=line.readLine())!=null){
				if(kw==null||kw.trim().length()==0) continue;
				if(spliteReg==null){ 
					kws.add(kw);
				}else{
					String[] ks=kw.split(spliteReg);
					Collections.addAll(kws,ks);
				}
			}
			line.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		return kws;
	}
	
	/**
	 * 将一个列表数据写入文件中。每条记录一行。
	 * @param file
	 * @param datas
	 * @throws IOException
	 */
	public  static void collectionToFile(File file,Collection<String> datas) throws IOException{
		FileWriter fw=new FileWriter(file);
		
		for(String data:datas){
			if(data==null||data.trim().length()==0) continue;
			fw.write(data);
			fw.write("\r\n");
		}
		fw.flush();
		fw.close();
	}
	
	/**
	 * 从一个列表中随机获取一个指定长度的列表。
	 * 如果指定的长度等于数据列表长度，那么会返回整个数据列表，但其排序会有所改变
	 * @param <T>
	 * @param datalist 数据列表
	 * @param limit 指定需要返回的列表长度
	 * @return
	 */
	public static <T> List<T>  RadomCollection(List<T> datalist, int limit){
		
		if(limit>datalist.size()) limit=datalist.size();
		long index=Math.round(Math.abs(Math.random())*(datalist.size()/2));
		
		long speed=Math.round(Math.random());
		speed=speed>0?1:-1;
		//if(Math.abs(speed)>(datalist.size()/2))speed=datalist.size()/2;
		
		
		if(index>=datalist.size()) index=0;
		
		ArrayList<T> list=new ArrayList<T>(limit);
		if(datalist==null||datalist.isEmpty()) return list;
		while(limit-->0){
			
			if(index<0) index=datalist.size()-1;
			if(index+1>datalist.size()) index=0;
		
			list.add(datalist.get((int)index));
			index+=speed;
		}
		
		
		return list;
	}
	
	public static String toLengthStr(String str,int length){
		if(str!=null&&str.length()<length){
			StringBuilder sb=new StringBuilder(str);
			for(int i=0;i<=length-str.length();i++){
				sb.append(" ");
			}
			return sb.toString();
		}
		return str;
	}
	
	public static  String getGoogleCIDFromURL(String url){
		if(url==null||url.trim().length()<10) return null;
		int index=url.indexOf("cid=");
		if(index>-1&&index<url.length()-5){
			int end=url.indexOf("&",index+4);
			if(end>0){
				return url.substring(index+4,end);
			}else{
				return url.substring(index+4);
			}
		}
		return null;
	}
	
	/**
	 * 将一个输入流转化成一个字符串
	 * @param is
	 * @param charset
	 * @return
	 * @throws IOException
	 */
	public static String getInputStreamString(InputStream is,String charset) throws IOException{
		String line;
    	StringBuilder builder = new StringBuilder();
    	BufferedReader reader=null;
    	if(charset!=null&&!charset.contains("utf"))  
    		reader= new BufferedReader(new InputStreamReader(is,charset));
    	else
    		reader= new BufferedReader(new InputStreamReader(is));
    	
    	try{
	    	while((line = reader.readLine()) != null) {
	    		builder.append(line);
	    	}
    	}finally{
	    	is.close();
	    	reader.close();
    	}
    	return builder.toString();
	}
	
	
	
	
	

	
	/**
	 * 将一个指定的列表，拆成几个指定长度的列表。
	 * @param <T>
	 * @param datalist 数据列表
	 * @param perNum 每个列表的长度
	 * @return
	 */
	public static <T> Collection<Collection<T>> splitCollection(Collection<T> datalist, Integer perNum){
			if(datalist==null||datalist.isEmpty()) return new ArrayList<Collection<T>>(0);
			
			int num=38;
			if(perNum!=null&&perNum>1){
				num=perNum;
			}
			
			Collection<Collection<T>> c=new ArrayList<Collection<T>>();
			
			int i=0;
			Collection<T> lc=null;
			for(T x:datalist){
				if(i++%num==0){
					if(lc!=null) c.add(lc);
					lc=new ArrayList<T>();
				}
				lc.add(x);
			}
			if(lc!=null&&!lc.isEmpty()) c.add(lc);
			return c;
	}
	
	/**
	 * 将一个字符串转化为一个数字。如果字符串中存在非数字部分，则只获取最早出现的数字部分。
	 * 可以处理小数部分。
	 * @param numStr 需要处理的字符串
	 * @return 返回数字。
	 */
	public static Number getNumber(String numStr){
		if(numStr==null||numStr.trim().length()<1)return 0;
		Pattern numberPattern=Pattern.compile("(\\d+(\\.\\d+)?)");
		Matcher match=numberPattern.matcher(numStr);
		if(match.find()){
			String group1=match.group();
			return Double.valueOf(group1);
		}
		return 0;
	}
	
	/**
	 * 获取标题。以标点符号作为判断依据。获取一段文本，直到遇到标点符号或数字。如果以标点符号或数字开头，
	 * 该部分标点符号及数字会被忽略。
	 * @param str
	 * @return
	 */
	public static String getTitle(String str){
		if(str==null) return "";
		Pattern titlePattern=Pattern.compile("[\\w\u4e00-\u9fa5]+");
		Matcher match=titlePattern.matcher(str);
		if(match.find()){
			return match.group();
		}
		return "";
	}
	
	public static void main(String[] args) {
		System.out.println("名人苑宾馆".replace("宾馆","222"));// ==名人苑宾馆
	}
	
}
