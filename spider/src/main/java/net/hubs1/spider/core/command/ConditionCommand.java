package net.hubs1.spider.core.command;

import java.util.Map;
import java.util.logging.Logger;

import net.hubs1.spider.core.BaseCommand;
import net.hubs1.spider.util.ContentUtil;

public class ConditionCommand extends BaseCommand{
	public ConditionCommand(int lineNum, String commandName, String[] params,String[] vars) {
		super(lineNum, commandName, params,vars);
	}

//	private static Logger log=Logger.getLogger("ConditionCommand");
	

	public void condition(String...params) {
		String[] mc=params[0].trim().split("\\|\\|");
		boolean isOK=true;
		for(String m:mc){
			if(!isOK) isOK=true; 
			String[] rules=m.trim().split(",,");
			for(String rule:rules){
				String[] od=getData(rule);
				if(!compare(od[0],od[1],od[2])){
					isOK=false; 
					break;
				}
			}
			if(isOK) break;
		}
	}
	
	public boolean test(){
		String[] mc=this.paramStrs[0].trim().split("\\|\\|");
		boolean isOK=true;
		for(String m:mc){
			if(!isOK) isOK=true; 
			String[] rules=m.trim().split(",,");
			for(String rule:rules){
				String[] od=getData(rule);
				if(!compare(od[0],od[1],od[2])){
					isOK=false; 
					break;
				}
			}
			if(isOK) break;
		}
		return isOK;
	}
	
	private boolean compare(String value,String opt,String value2){
		
		if(value==null||value2==null) return false;
		if("=".equals(opt)){
			return value.equals(value2);
		}
		
		int i=value.compareTo(value2);
		
		if(">".equals(opt)&&i>0) return true;
		
		if("<".equals(opt)&&i<0) return true;
			
		return false;
	}
	
	private String[] getData(String rule){
		String[] data=rule.split(" ");
		if(data[0].startsWith("$")) data[0]=(String)getVarData(data[0].substring(1));
		if(data[2].startsWith("$")) data[2]=(String)getVarData(data[2].substring(1));
		return data;
	}

	
}
