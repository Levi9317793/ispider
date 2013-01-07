package net.hubs1.bijia.extract;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

public class ToolCommand  {
	private static Properties PY=null;
	/**
	 * params[0],是否去首字母，params[1]：拼音的分隔符，不设置，默认为“_”.
	 * @param chinese 需要设置拼音的中文
	 * @param isFirstChar 是否只取拼音首字母
	 * @return
	 */
	public  static String pinyin(String input,String[] params){
		if(PY==null){
			PY=new Properties();
			
			try {
				InputStream is=ToolCommand.class.getResource("py.properties").openStream();
				PY.load(is);
				is.close();
			} catch (IOException e) {
			}
		}
		boolean isAll=params!=null&&params.length>0?"true".equalsIgnoreCase(params[0]):true;
		char[] hotelnames=((String)input).toCharArray();
		StringBuilder sb=new StringBuilder();
		for(char c:hotelnames){
			if(c>=0x3400 && c<=0x9FBF){//判断是否中文
				//获取拼音
				String pinyin=PY.getProperty(String.valueOf(c));
				if(null==pinyin) continue;
				String[] lsstr=pinyin.split(" ");//多音字的处理
				//获取正确的拼音。
				
				if(!isAll){
					if(lsstr!=null&&lsstr.length>0) sb.append(lsstr[0].charAt(0)).append("_");//目前只能取第一种读音
				}else{
					if(lsstr!=null&&lsstr.length>0)sb.append(lsstr[0]).append("_");
				}
			}else{
				sb.append(c).append("_");
			}
		}
		if(sb.length()>0) sb.deleteCharAt(sb.length()-1);
		return sb.toString();
	}

}
