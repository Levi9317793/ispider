package net.hubs1.spider.util;

import java.util.List;
import java.util.Map;

import net.hubs1.spider.core.BaseCommand;

public class ContentUtil {
	/**
	 * 在上下文中获取指定的参数的值，并将其转化为字符串。
	 * 如果无法转化，将返回null。
	 * @param var
	 * @param content
	 * @return
	 */
	public static String getHtmlPage(String var,Map<String,Object>... content){
		if(var==null) var="HtmlPage";
		Object sto=getVarData(var,content);
		if(sto!=null){
			if(sto instanceof String) 
				return (String)sto;
			if(sto instanceof byte[])
				return new String((byte[])sto);
		}
		return null;
	}
	
	/**
	 * 获取给定上下文环境中的当前值。
	 * @param content
	 * @return
	 */
	public static Object getCurrentData(Map<String,Object>... content){
		return getVarData(BaseCommand.CURRENT_VALUE,content);
	}
	
	/**
	 * 在给定的上下文中获取指定的变量的值
	 * @param var
	 * @param content
	 * @return
	 */
	public static Object getVarData(String var,Map<String,Object>... contents){
		for(Map<String,Object> content:contents){
			if(content.containsKey(var)){
				return content.get(var);
			}
		}
		return null;
	}
	
	/**
	 * @param var
	 * @param content
	 * @return
	 */
	public static String getString(String var,Map<String,Object>... content){
		if(var==null) var=BaseCommand.CURRENT_VALUE;
		Object sto=getVarData(var,content);
		if(sto!=null){
			if(sto instanceof String) 
				return (String)sto;
			if(sto instanceof byte[])
				return new String((byte[])sto);
			if(sto instanceof List){
				List<String> l=(List<String>)sto;
				if(l.size()==1) return l.get(0);
			}
		}
		return null;
	}
	
//	public static String getVarValue(String var,Map<String,Object> content){
//		String v=var.startsWith("$")?(String)content.get(var.substring(1)):var;
//		return v;
//	}
	
	public static void putContent(String[] vars,Object value,Map<String,Object> content){
		for(String var:vars){
			content.put(var, value);
		}
	}
	
	
	
	public static Map<String,String> getRequestParam(Map<String,Object>... content){
		return (Map<String,String>)getVarData("RequestParams",content);
	}
	
	
	public static Object getParamVale(String param,Map<String,Object>... content){
		if(param!=null){
			if(param.trim().startsWith("\\$")){
				return param.trim().substring(1);
			}else
				if(param.trim().startsWith("$")){
					return getVarData(param.trim().substring(1),content);
				}else{
					return param;
				}
		}
		return null;
	}
	
	
}
