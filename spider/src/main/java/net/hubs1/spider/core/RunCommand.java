package net.hubs1.spider.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import net.hubs1.spider.core.command.ConditionCommand;
import net.hubs1.spider.core.command.EndConditionCommand;
import net.hubs1.spider.core.command.RequestCommand;

public class RunCommand {
	
	private static Logger log=Logger.getLogger(RunCommand.class);
	
	public static Map<String,Object> execute(List<Command> commands,Map<String,List<Command>> blocks)throws RuntimeException{
		Map<String,Object> map=new HashMap<String,Object>();
		Map<String,Object> gobal=new HashMap<String,Object>();
		execute(commands,blocks,map,gobal);
		return map;
	}
	
	
	/**
	 * 该execute方法返回的map对象，经过合并的。
	 * @param commands
	 * @param blocks
	 * @return
	 * @throws RuntimeException
	 */
	public static Map<String,Object> executeBlock(List<Command> commands,Map<String,List<Command>> blocks)throws RuntimeException{
		Map<String,Object> map=new HashMap<String,Object>();
		Map<String,Object> gobal=new HashMap<String,Object>();
		execute(commands,blocks,map,gobal);
		map.putAll(gobal);
		return map;
	}
	
	public static  void execute(List<Command> commands,Map<String,List<Command>> blocks,Map<String,Object> content,Map<String,Object> gobalContent)throws RuntimeException{
		Integer condition=null;
		String cc=null;
		for(int index=0;index<commands.size();index++){
			//log.debug("the line is:"+index+","+commands.get(index));
			BaseCommand command=(BaseCommand)commands.get(index);
			
			command.setContext(content, gobalContent);
			command.setCommandMap(blocks);
			try{
				
				if("test".equalsIgnoreCase(command.commandName())){
					command.execute();
					if("stop".equals(content.get("sys_test_result"))){
						break;
					}
				}
				
				if(command instanceof RequestCommand){
					RequestCommand rc=(RequestCommand)command;
					List<String> urls=rc.mutiRequest();
					
					if(urls.size()>1){
						List<Command> commandList=copyList((List<Command>)commands, index+1);
						for(String url:urls){
							rc.execute(url);
							execute(commandList,blocks,content,gobalContent);// 递归调用
						}
					}else{
						if(urls!=null&&urls.size()==1)
							rc.execute(urls.get(0));
						else{
							log.error(" don't set url ");
						}
					}
				}else{
					command.execute();
				}
			}catch(Exception e){
				throw new RuntimeException(" CommandName is "+command.commandName()+"  lineNum is:"+command.lineNum(),e);
			}
		}
	}
	
	private static  <T> List<T>  copyList(List<T> src,int start){
		List<T> list=new ArrayList<T>();
		for(int i=start;i<src.size();i++){
			list.add(src.get(i));
		}
		return list;
	}
}
