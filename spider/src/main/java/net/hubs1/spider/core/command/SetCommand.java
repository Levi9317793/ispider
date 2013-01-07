package net.hubs1.spider.core.command;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import net.hubs1.spider.core.BaseCommand;

public class SetCommand extends BaseCommand  {
	
	public SetCommand(int lineNum, String commandName, String[] params,String[] vars) {
		super(lineNum, commandName, params,vars);
	}
	private  Logger log=Logger.getLogger(SetCommand.class);
	
	public void setGobal(String[] params){
		int num=params==null?0:params.length;
		switch(num){
			case 0: 
				setGobalVar(this.inputData);
				break;
			case 1:
				setGobalVar( params[0]);
				break;
			case 2:
				setGobalVar(params[1], (String)params[0]);
				break;
			default:
				log.error("setgobal command is error,please check param,the line number is :"+this.lineNum);
		}
	}
	
	public void setRequestParam(String[] params){
		HashMap<String,String> requestParam=(HashMap<String,String>)getVarData("RequestParams");
		if(requestParam==null){
			requestParam=new HashMap<String,String>();
			setContextVar("RequestParams",requestParam);//content.put("RequestParams", requestParam);
		}
		
		if(params.length==1){
			setParamters(requestParam,params[0]);
		}else{
			requestParam.put(params[0], params[1]);
		}
	}
	
	public void removerequestparam(String... params){
		HashMap<String,String> requestParam=(HashMap<String,String>)getVarData("RequestParams");
		if(requestParam!=null){
			if(params==null||params.length==0){
				setContextVar("RequestParams",null);
			}else{
				for(String param:params){
					requestParam.remove(param);
				}
			}
		}
	}
	
	public void removerequesthead(String... params){
		HashMap<String,String> requestHead=(HashMap<String,String>)getVarData("RequestHead");
		if(requestHead!=null){
			if(params==null||params.length==0){
				setContextVar("RequestHead",null);
			}else{
				for(String param:params){
					requestHead.remove(param);
				}
			}
		}
	}
	
	/**
	 * 设置
	 * @param input
	 * @param params
	 * @param content
	 */
	public void setrequesthead(String... params){
		HashMap<String,String> requestHead=(HashMap<String,String>)getVarData("RequestHead");
		if(requestHead==null){
			requestHead=new HashMap<String,String>();
			setContextVar("RequestHead", requestHead);
		}
		
		if(params.length==2)
			 requestHead.put(params[0],params[1]);
	}
	
	private void setParamters(HashMap<String,String> requestParam,String params){
		if(params==null) return;
		String html=(String)this.inputData;
		if(html!=null){
			Document doc=Jsoup.parse(html);
			String[] paramses=params.split(",,");
			Elements root=new Elements ();
			root.add(doc);
			for(String p:paramses){
				if(root==null||root.isEmpty()) break;
				root=root.select(p);
			}
			Iterator<Element> ite=root.iterator();
			while(ite.hasNext()){
				Element e=ite.next();
				String val=e.val();
				String vn=e.attr("name");
				if(val!=null&&vn!=null&&vn.trim().length()>0)
					requestParam.put(vn, e.val());
			}
		}
	}
	
	public void set(String... params){
		Object obj=this.inputData;
		if(params!=null&&params.length==1){
			if("NullNoSet".equalsIgnoreCase(params[0])){
				if(obj!=null){
					putResult2Context( obj);
				}
				return;
			}
			if(obj!=null&&(obj instanceof Object[] || obj instanceof List)&&params[0].matches("\\d+")){
				Object r=null;
				if(obj instanceof Object[]) r=((Object[])obj)[Integer.parseInt(params[0])];
				if(obj instanceof List) r=((List)obj).get(Integer.parseInt(params[0]));
				putResult2Context(r);
			}else{
				putResult2Context(params[0]);
			}
		}else{
			putResult2Context( obj);
		}
	}
	
	public void cal(String... params){
		String i=(String)this.inputData;
		Integer source=Integer.parseInt(i);
		for(String s:params){
			char c=s.charAt(0);
			String num=s.substring(1);
			if(num.startsWith("$")) num=(String)this.getVarData(num.substring(1));
			
			Integer t=Integer.parseInt(num);
			switch(c){
				case '+':source=source+t;break;
				case '-':source=source-t;break;
				case '*':source=source*t;break;
				case '/':source=t==0?0:source/t;break;
				case '%':source=t==0?0:source%t;break;
				default:break;
			}
		}
	}
	
	public void removeset(Object... params){
		this.blockContext.remove(this.inputVar);
	}
}
