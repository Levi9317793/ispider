package net.hubs1.spider.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

public  class SpiderUtil {
	public static String exception2String(Throwable e){
		StringWriter sw=new StringWriter();
		PrintWriter pw=new PrintWriter(sw);
		e.printStackTrace(pw);
		return sw.toString();
	}
	
	
	public static String array2Str(String[] strs){
		if(strs==null) return null;
		if(strs.length==0) return "";
		StringBuilder sb=new StringBuilder("[");
		for(String str:strs){
			sb.append(str).append(",");
		}
		if(sb.length()>1) sb.deleteCharAt(sb.length()-1);
		sb.append("]");
		return sb.toString();
	}
	/**
	 * @param url
	 * @param rowkeyPrefix
	 * @param deleteUrlParam
	 * @param deleteExt
	 * @return
	 */
	public static String url2RowKey(String url,String rowkeyPrefix,boolean deleteUrlParam,boolean deleteExt){
		int i=-1;
		if((i=url.indexOf("//"))!=-1){
			url=url.substring(i+2);
		}
		
		if(deleteUrlParam){
			int index=url.indexOf("?");
			if(index>0) url=url.substring(0,index);
		}
		
		String rowkey=rowkeyPrefix;
		if(!url.startsWith("/")){
			url=url.substring(url.indexOf("/"));
		}
		
		int haveExt=-1;
		if(deleteExt)
			 haveExt=url.lastIndexOf(".");
		
		if(haveExt>0)
			rowkey=rowkey+url.substring(0,haveExt);
		else
			rowkey=rowkey+url;
		
		
		rowkey=rowkey.replaceAll("/", ".");
		return rowkey;
	}
}
