package net.hubs1.spider.core;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.hubs1.spider.util.SpiderUtil;

import org.apache.log4j.Logger;

public class CommandParse {
	private static Logger log=Logger.getLogger(CommandParse.class);
	
	
	
	public static Map<String,List<Command>> parse(String formatfile){
		List<String> commandList=readCommand(new File(formatfile));
		return parse(commandList);
	}
	
	public static Map<String,List<Command>> parseFromContent(String fileContent){
		List<String> commandList=readCommand(fileContent);
		return parse(commandList);
	}
	
	
	public static String testFormat(String fileContent){
		List<String> commandList=readCommand(fileContent);
		return parseTest(commandList);
	}
	
	public static Map<String,List<Command>> parse(List<String> commandList){
		Map<String,List<Command>> commandMap=new HashMap<String, List<Command>>();
		String blockName=null;
		for(int i=0;i<commandList.size();i++){
			int lineNum=i+1;
			String line=commandList.get(i).trim();
			if(line.startsWith("#")) continue;
			
			if(line==null||line.trim().length()==0) continue;
			
			if(line.startsWith("block_")){
				if(blockName!=null){
					log.error(" other block is not end,line number is:"+lineNum);
					break;
				}
				blockName=line.substring(line.indexOf("_")+1);
				continue;
			}
			
			if("endblock".equals(line)){
				if(blockName==null){
					log.error(" error tag endblock, line number is:"+lineNum);
					break;
				}
				blockName=null;
				continue;
			}
			
			if(blockName!=null){
				List<Command> cl=commandMap.get(blockName);
				if(cl==null){
					cl=new ArrayList<Command>();
					commandMap.put(blockName,cl);
				}
				int index=line.indexOf("=");
				String vars=null;
				if(line.trim().startsWith("$")&&index>0){
					vars=line.substring(0,index);
					line=line.substring(index+1);
				}
				
				index=line.indexOf(":");
				if(index!=-1){
					String commandName=line.substring(0,index);
					if(commandName==null||commandName.length()<2){
						log.error(" command init is fail,the line is:"+line);
						continue;
					}
					try {
					
						cl.add(BaseCommand.checkSet(lineNum, commandName, (index<line.length()?line.substring(index+1):null),vars));
					} catch (Exception e) {
						log.error(" command init is fail:",e);
					}
				}else{
					log.error("parse command is error,loss : ,line number is:"+lineNum);
				}
			}else{
				log.error("file format is error,it is not in block, line number is:"+lineNum);
				break;
			}
			
		}
		return commandMap;
	}
	
	/**
	 * 模版文件测试。 如果返回null表示成功，否则表示有错误，
	 * @param commandList
	 * @return 错误信息。
	 */
	public static String parseTest(List<String> commandList){
		Map<String,List<Command>> commandMap=new HashMap<String, List<Command>>();
		String blockName=null;
		StringBuilder sb=new StringBuilder();
		for(int i=0;i<commandList.size();i++){
			int lineNum=i+1;
			String line=commandList.get(i).trim();
			if(line.startsWith("#")) continue;
			
			if(line==null||line.trim().length()==0) continue;
			
			if(line.startsWith("block_")){
				if(blockName!=null){
					sb.append("["+blockName+"]块尚未结束,在第:"+lineNum).append("行\r\n");
				}else{
					blockName=line.substring(line.indexOf("_")+1);
				}
				continue;
			}
			
			if("endblock".equals(line)){
				if(blockName==null){
					sb.append("没有定义 block_, 请定义块的名称，在第:"+lineNum).append("行\r\n");
				}else{
					blockName=null;
				}
				continue;
			}
			
			if(blockName!=null){
				List<Command> cl=commandMap.get(blockName);
				if(cl==null){
					cl=new ArrayList<Command>();
					commandMap.put(blockName,cl);
				}
				int index=line.indexOf("=");
				String vars=null;
				if(line.trim().startsWith("$")&&index>0){
					vars=line.substring(0,index);
					line=line.substring(index+1);
				}
				
				index=line.indexOf(":");
				if(index!=-1){
					String commandName=line.substring(0,index);
					
					if(!BaseCommand.exists(commandName)){
						sb.append(" 命令 ["+commandName+"] 不存在,在第 "+lineNum).append("行\r\n");
					}
					
					try {
						cl.add(BaseCommand.checkSet(lineNum, commandName, (index<line.length()?line.substring(index+1):null),vars));
					} catch (Exception e) {
						sb.append("在初始化命令时错误，请检查命令的参数设置是否正确，具体错误原因如下（在第"+lineNum+"行）：\r\n"+SpiderUtil.exception2String(e)).append("\r\n");
					}
				}else{
					sb.append(" 解析命令错误,缺失:号，主要必须是英文半角， 在第"+lineNum).append("行\r\n");
				}
			}else{
				sb.append(" 发现未被定义在块内的命令, 在第:"+lineNum).append("行\r\n");
			}
			
		}
		return  sb.length()>0?sb.toString():null;
	}

	
	/**
	 * 从一个文件中读取记录，并将其放入一个列表中
	 * @param file 需要读取的文件
	 * @param spliteReg 每行文件的分隔符，如果有分隔符，则一行有可能对应列表中的多条记录
	 * @param deleteRepeat 是否删除返回列表中的重复记录
	 * @return
	 */
	private  static List<String> readCommand(File file) {
		List<String> kws=new ArrayList<String>();
		try{
			LineNumberReader line=new LineNumberReader(new FileReader(file));
			String kw=null;
			while((kw=line.readLine())!=null){
				if(kw.trim().length()==0){
					kw="";
				}
				kws.add(kw);
			}
			line.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		return kws;
	}
	
	
	private  static List<String> readCommand(String fileContent) {
		List<String> kws=new ArrayList<String>();
		try{
			LineNumberReader line=new LineNumberReader(new StringReader(fileContent));
			String kw=null;
			while((kw=line.readLine())!=null){
				if(kw.trim().length()==0){
					kw="";
				}
				kws.add(kw);
			}
			line.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		return kws;
	}
}
