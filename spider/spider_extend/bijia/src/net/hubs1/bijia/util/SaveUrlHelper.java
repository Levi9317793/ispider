package net.hubs1.bijia.util;


public class SaveUrlHelper {

	public String url2RowKey(String url,String rowkeyPrefix,boolean deleteUrlParam){
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
		
		int haveExt=url.lastIndexOf(".");
		if(haveExt>0) 
			rowkey=rowkey+url.substring(0,haveExt);
		else
			rowkey=rowkey+url;
		
		rowkey=rowkey.replaceAll("/", ".");
		return rowkey;
	}
	
	public static String rowkey2url(String rowKey, String prefix,String domain){
		String url = rowKey.substring(rowKey.indexOf(prefix)+prefix.length());
		url = domain + url.replace(".", "/");
		return url;
	}

}
