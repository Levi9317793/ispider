package net.hubs1.spider.core.command;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;

import net.hubs1.spider.core.BaseCommand;
import net.hubs1.spider.util.ContentUtil;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class JsonCommand extends BaseCommand {
	
	public JsonCommand(int lineNum, String commandName, String[] params,String[] vars) {
		super(lineNum, commandName, params,vars);
	}
	
	
	public void jsonstring(String... params){
		Object obj=this.inputData;
		if(obj==null){
			this.putResult2Context(null);
			return ;
		}
		
		String json=null;
		if(obj instanceof Map){
			json=map2JSONObject((Map)obj);
		}else{
			if(obj instanceof List)json=list2JSONList((List)obj);
		}
		this.putResult2Context(json);
		return ;
	}
	
	private String map2JSONObject(Map<String,Object> map){
		StringBuilder sb=new StringBuilder("{");
		for(Map.Entry<String, Object> entry:map.entrySet()){
			sb.append("\"").append(entry.getKey()).append("\":");
			if(entry.getValue()==null){
				sb.append("\"\",");
				continue;
			}
			if(entry.getValue() instanceof Map){
				String obj=map2JSONObject((Map<String,Object>)entry.getValue());
				sb.append(obj).append(",");
				continue;
			}
			if(entry.getValue() instanceof List){
				String obj=list2JSONList((List<Object>)entry.getValue());
				sb.append(obj).append(",");
				continue;
			}
			sb.append("\"").append(entry.getValue().toString()).append("\",");
		}
		if(sb.length()>1) sb.deleteCharAt(sb.length()-1);
		sb.append("}");
		return sb.toString();
	}
	
	private String list2JSONList(List<Object> list){
		StringBuilder sb=new StringBuilder("[");
		for(Object obj:list){
			if(obj==null){
				continue;
			}
			if(obj instanceof Map){
				String map=map2JSONObject((Map<String,Object>)obj);
				sb.append(map).append(",");
				continue;
			}
			if(obj instanceof List){
				String ll=list2JSONList((List<Object>)obj);
				sb.append(ll).append(",");
				continue;
			}
			sb.append("\"").append(obj.toString()).append("\",");
		}
		if(sb.length()>1) sb.deleteCharAt(sb.length()-1);
		sb.append("]");
		return sb.toString();
	}
	
	
	/**
	 * 将字符串转化为json对象。
	 * @param strings
	 */
	public void jsonobj(String...strings){
		Object obj=this.inputData;
		
		Object jo=JSONObject.fromObject(obj);
//		JSONArray.
		this.putResult2Context(jo);
	}
	
	/**
	 * 操作JSON对象。
	 * @param strings
	 */
	public void json(String...strings){
		Object jo=this.inputData;
		Object obj=null;
		for(String express:strings){
			if(express.matches("\\[\\d+\\]")){
				express=express.substring(0,express.length()-1);
				obj=((JSONArray)jo).get(Integer.parseInt(express));
				jo=obj;
			}else{
				obj=((JSONObject)jo).get(express);
				jo=obj;
			}
		}
		this.putResult2Context(obj);
	}
}
