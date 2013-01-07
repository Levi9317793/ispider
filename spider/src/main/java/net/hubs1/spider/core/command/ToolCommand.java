package net.hubs1.spider.core.command;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.log4j.Logger;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import net.hubs1.spider.core.BaseCommand;
import net.hubs1.spider.core.Command;
import net.hubs1.spider.core.RunCommand;
import net.hubs1.spider.util.SpiderUtil;

public class ToolCommand extends BaseCommand {
	private static Logger log=Logger.getLogger(ToolCommand.class);
	private static Properties PY=null;
	public ToolCommand(int lineNum, String commandName, String[] params,String[] vars) {
		super(lineNum, commandName, params,vars);
	}
	
	/**
	 * params[0],是否去首字母，params[1]：拼音的分隔符，不设置，默认为“_”.
	 * @param chinese 需要设置拼音的中文
	 * @param isFirstChar 是否只取拼音首字母
	 * @return
	 */
	public  void pinyin(String... params){
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
		if(this.inputData instanceof String){
			char[] hotelnames=((String)this.inputData).toCharArray();
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
			putResult2Context(sb.toString());
	   }else{
		   putResult2Context(null);
	   }
	}
	
	public void list(Object... params){
		ArrayList al=new ArrayList();
		if(this.inputData instanceof Collection){
			al.addAll((Collection)this.inputData);
		}else{
			al.add(this.inputData);
		}

		for(Object param:params ){
			if(param instanceof Collection){
				al.addAll((Collection)param);
			}else{
				al.add(param);
			}
		}
		putResult2Context( al);
	}
	
	public void createurl(String... params){
		String url=(String)this.inputData;
		String reg=params[0];
		String minPageStr=params[1];
		String maxPageStr=params[2];
		String replaceStr=params.length==4?params[3]:null;
		
		if(minPageStr==null||maxPageStr==null||reg==null){
			log.error("params is error ,please set reg, minPage,maxPage,that required!");
			this.putResult2Context(null);
		}
		
		int minPage=0;
		int maxPage=0;
		
		Pattern p=Pattern.compile(reg);
		ArrayList<String> al=new ArrayList<String>();
		
		if(minPageStr.matches("^\\d+$")&&minPageStr.matches("^\\d+$")){
			minPage=Integer.parseInt(minPageStr);
			maxPage=Integer.parseInt(maxPageStr);
			for(int i=minPage;i<=maxPage;i++){
				Matcher m=p.matcher(url);
				if(m.find()){
					String page=String.valueOf(i);
					if(replaceStr!=null) page=replaceStr.replaceAll("#page#",String.valueOf(i));
					al.add(m.replaceAll(page));
				}
			}
		}else{
			for(char ch=minPageStr.charAt(0);ch<=maxPageStr.charAt(0);ch++){
				Matcher m=p.matcher(url);
				if(m.find()){
					String page=String.valueOf(ch);
					if(replaceStr!=null) page=replaceStr.replaceAll("#page#",String.valueOf(ch));
					al.add(m.replaceAll(page));
				}
			}
		}
		this.putResult2Context(al.isEmpty()?null:al);
	}
	
	public void url2rowkey(String... params){
		String url=null;
		int index=0;
		if(params.length==4){
			url=params[index++];
		}else{
			url=(String)this.inputData;
		}
		String rowkeyPrefix=params[index++];
		String deleteUrlParam=params[index++];
		String deleteExt=params[index++];
		
		String rowkey=SpiderUtil.url2RowKey(url, rowkeyPrefix, "true".equalsIgnoreCase(deleteUrlParam), "true".equalsIgnoreCase(deleteExt));
		this.putResult2Context(rowkey);
	}
	
	
	public void rowkey2url(String...params){
		String head=params[0];
		String tail=params.length>1?params[1]:"";
		
		String url=(String)this.inputData;
		
		int first=url.indexOf(".");
		int second=url.indexOf(".",first+1);
		int three=url.indexOf(".",second+1);
		
		String language=null;
		if(three>0){
			language=url.substring(second+1,three);
		}
		
		if("en".equals(language)||"cn".equals(language)){
			url=url.substring(three);
		}else{
			url=url.substring(second);
		}
		
		url=url.replaceAll("\\.","/");
		url=head+url+tail;
		this.putResult2Context(url);
	}
	
	 // MD5加码。32位  
	 public  void md5(String... params) {  
		 String inStr=(String)this.inputData;
		 
		 if(inStr==null) inStr="";
		 
	     if(params!=null&&params.length>0){
			 for(String param:params){
				 if(param!=null)
					 inStr=inStr+param;
			 }
          }
	     
	     if(inStr==null||inStr.trim().length()==0){
	    	 this.putResult2Context(null);
	    	 return;
	     }
	     
		  MessageDigest md5 = null;  
		  try {  
		   md5 = MessageDigest.getInstance("MD5");  
		  } catch (Exception e) {  
			  log.error(e);
			  this.putResult2Context(null);
		  }  
		  char[] charArray = inStr.toCharArray();  
		  byte[] byteArray = new byte[charArray.length];  
		  
		  for (int i = 0; i < charArray.length; i++)  
		   byteArray[i] = (byte) charArray[i];  
		  
		  byte[] md5Bytes = md5.digest(byteArray);  
		  
		  StringBuffer hexValue = new StringBuffer();  
		  
		  for (int i = 0; i < md5Bytes.length; i++) {  
		   int val = ((int) md5Bytes[i]) & 0xff;  
		   if (val < 16)  
		    hexValue.append("0");  
		    hexValue.append(Integer.toHexString(val));  
		  }
		  this.putResult2Context(hexValue.toString());  
	}
	 
	 public void loop(String... block){
		 String isTurn=(String)this.inputData;
		 String isDoWhile=block.length==2&&"dowhile".equalsIgnoreCase(block[1])?"true":"false";
		 
		 if(block[0].startsWith("!")){
			 String blockName=block[0].substring(1);
			 Map<String,List<Command>> blocks=this.commandMap();
			 
			 if("true".equalsIgnoreCase(isDoWhile)){
				isTurn="true";
			 }
			 
			 while("true".equalsIgnoreCase(isTurn)){
				 this.setContextVar(this.inputVar, "false");//设置为false，防止无限循环
				 RunCommand.execute(blocks.get(blockName),blocks,this.blockContext,this.taskContext);
				 isTurn=(String)this.getVarData(this.inputVar);
			 }
		 }
	 }
	 
	 public void test(String...strings){
		String test=(String)this.inputData;
		
		String rm=strings!=null&&strings.length>0?strings[0]:null;
		if("true".equalsIgnoreCase(test)){
			this.setContextVar("sys_test_result", "stop");
			
		}
		if("remove".equalsIgnoreCase(rm)){
			this.blockContext.remove(this.inputVar);
			this.taskContext.remove(this.inputVar);
		}
	 }
	 
	 public void runjs(String...strings){
		 String js=(String)this.inputData;
		 
		 js=js.replaceFirst("<script[-+_*=#@!;\\/ 0-9a-zA-Z]*>", "");
		 js=js.replace("</script>", "");
		 
		 ScriptEngineManager manager = new ScriptEngineManager();  
	     ScriptEngine engine = manager.getEngineByExtension("js"); 
	     
	     Object result=null;
	     try {
			 result=engine.eval(js);
		 } catch (ScriptException e) {
			log.error("run javascript is error:"+js,e);
		 }
		this.putResult2Context(result);
	 }
	 
	 public void css(String...strings){
		 
	 }
	 
	 public void calculate(String...strings){
		 String s=null;
		 if(!"current".equals(this.inputVar)){
			 s=(String)this.inputData;
		 }

		 int index=0;
		 int firstData=s==null?Integer.parseInt(strings[index++]):Integer.parseInt(s);
		 int secondData=0;
		 char optSyboml='u';
		 while(index<strings.length){
				optSyboml=strings[index++].charAt(0);
				secondData=Integer.parseInt(strings[index++]);
				
				switch(optSyboml){
					case '+':firstData=firstData+secondData;break;
					case '-':firstData=firstData-secondData;break;
					case '*':firstData=firstData*secondData;break;
					case '/':firstData=firstData/secondData;break;
					case '%':firstData=firstData%secondData;break;
					default:  return;
				}
		 }
		 this.putResult2Context(String.valueOf(firstData));
	 }
	 
	 /**
	  * 将列表转化为字符串表示。
	  * td之间的数据以\t(制表符)分隔，tr之间以\r\n分隔
	 * @param strings
	 */
	public void table2String(String...strings){
		String deleteFirst=strings!=null&&strings.length>0?strings[0]:null;
		boolean iSDeleteFirst="delete".equalsIgnoreCase(deleteFirst);
		
		 Object e=this.inputData;
		 if(e instanceof Element){
			Element table=(Element)e;
			if("table".equalsIgnoreCase(table.nodeName())){
				StringBuilder sb=new StringBuilder();
				Elements trs=table.select("tr");
				for(int i=0;i<trs.size();i++){
					if(iSDeleteFirst&&i==0) continue;
					Element tr=trs.get(i);
					Elements tds=tr.children();
					if(tds!=null&&!tds.isEmpty()){
						Iterator<Element> tdIte=tds.iterator();
						while(tdIte.hasNext()){
							Element td=tdIte.next();
							sb.append(td.text()).append("\t");
						}
						sb.append("\r\n");
					}
				}
				this.putResult2Context(sb.toString());
				return;
			}
		 }
		 
		 this.putResult2Context(null);
	 }
}
