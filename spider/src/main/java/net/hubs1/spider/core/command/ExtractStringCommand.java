package net.hubs1.spider.core.command;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.hubs1.spider.core.BaseCommand;
import net.hubs1.spider.util.DataUtil;

public class ExtractStringCommand extends BaseCommand {

	public ExtractStringCommand(int lineNum, String commandName, String[] params,String[] vars) {
		super(lineNum, commandName, params,vars);
	}

	/**
	 * 替换
	 * @param content
	 */
	public void replace(String... params) {
		String dv=params.length>2&&params.length%2==1?params[params.length-1]:null;
		
		Object obj=this.inputData;
		if(obj==null){
			putResult2Context(dv);
			return;
		}
		
		if(obj instanceof String){
			String data=(String)obj;
			int maxIndex=params.length>2&&params.length%2==1?(params.length-1):params.length;
			int i=0;
			while(i<maxIndex){
				Pattern p=Pattern.compile(params[i++]);
				Matcher m=p.matcher(data);
				if(m.find()){
					dv=m.replaceAll(params[i]);
					data=dv;
				}
				i++;
			}
			putResult2Context(dv);
			return;
		}
		
		String reg=(String)params[0];
		String reStr=params.length>1?params[1]:"";
		if(reStr==null) reStr="";
		Pattern p=Pattern.compile(reg);
		if(obj instanceof List){
			List list=(List)obj;
			for(int i=0;i<list.size();i++){
				String str=(String)list.get(i);
				Matcher m=p.matcher(str);
				String r=dv;
				if(m.find()){
					r=m.replaceAll(reStr);
				}
				list.set(i, r);
			}
			putResult2Context(list);
		}
	}
	
	
	/**
	 * 替换
	 * @param content
	 */
	public void removestr(String... params) {
		Object obj=this.inputData;
		if(obj instanceof String){
			String dv=(String)obj;
			for(String str:params){
				dv=dv.replace(str,"");
			}
			
			putResult2Context(dv);
			return;
		}
		
		if(obj instanceof List){
			List list=(List)obj;
			for(int i=0;i<list.size();i++){
				String r=(String)list.get(i);
				for(String str:params){
					r=r.replace(str,"");
				}
				list.set(i, r);
			}
			putResult2Context(list);
		}
	}
	
	
	private <T> List<T> array2List(T[] as){
		
		if(as!=null){
			ArrayList<T> al=new ArrayList<T>(as.length);
			for(T t:as){
				al.add(t);
			}
			return al;
		}
		return null;
	}
	
	/**
	 * 
	 * @param content
	 */
	public void split(String... params) {
		String reg=params[0];
		
		Object obj=this.inputData;
		if(obj==null){
			this.putResult2Context(null);
			return;
		}
			
		if(obj instanceof String){
			String[] strs=((String)obj).split(reg);
			List<String> list= this.<String>array2List(strs);
			this.putResult2Context(list);//ContentUtil.putContent(vars, list, content);
			return;
		}
		
		if(obj instanceof List){
			List list=(List)obj;
			for(int i=0;i<list.size();i++){
				String str=(String)list.get(i);
				list.set(i, str.split(reg));
			}
			this.putResult2Context(list);//ContentUtil.putContent(vars, list, content);
		}
	}

	/**
	 * 子串
	 * @param content
	 */
	public void substring(String... params) {
		Object obj=this.inputData;
		if(obj==null){
			this.putResult2Context(null);//ContentUtil.putContent(vars, null, content);
			return;
		}
		
		if(obj instanceof String){
			String str=(String)obj;
			if(str.length()==0){
				this.putResult2Context(null);//ContentUtil.putContent(vars, null, content);
				return;
			}
			int index=0;
			int endIndex=-99;
			if(params.length>=1){
				String p=params[0];
				if(p.length()==0){
					index=0;
				}else{
					index=str.indexOf(p);
					if(index>-1) 
						index=index+p.length();
				}
			}
			
			if(index!=-1){
				str=str.substring(index);
				if(params.length==2) endIndex=str.indexOf(params[1]);
				if(endIndex==-1) 
					str=null;
				else
					if(endIndex!=-99) str=str.substring(0,endIndex);
				this.putResult2Context(str);//ContentUtil.putContent(vars, str, content);
			}else{
				this.putResult2Context(null);//ContentUtil.putContent(vars, null, content);
			}
			return;
		}
		
		if(obj instanceof List){
			List list=(List)obj;
			ArrayList<String> ar=new ArrayList<String>(list.size());
			for(int i=0;i<list.size();i++){
				String str=(String)list.get(i);
				if(str.length()==0) continue;
				int index=0;
				int endIndex=str.length();
				if(params.length==1){
					index=str.indexOf(params[0]);
					if(index==-1) continue;
				}
				if(params.length==2){
					endIndex=str.indexOf(params[1]);
					if(endIndex==-1) continue ;
				}
				ar.add(i,str.substring(index,endIndex));
			}
			this.putResult2Context(list);//ContentUtil.putContent(vars, list, content);
		}
	}

	/**
	 * 字符串连接
	 * @param content
	 */
	public void join(String... params) {
		Object obj=this.inputData;
		
		if(obj==null) obj="";
		
		String rStr=params!=null&&params.length>0?params[0]:"";
		if(rStr==null) rStr="";
		
		boolean isRight=params!=null&&params.length>1?("right".equalsIgnoreCase(params[1])):true;
		String splitor=params!=null&&params.length>2?params[2]:",";
		
		if(rStr.length()>0){
			rStr=isRight?(splitor+rStr):(rStr+splitor);
		}
		
//		if(params!=null&&params.length>0&&params[0].trim().startsWith("$")){
//			rStr=(String)params[0];
//		}
		
		if(obj instanceof String){
			String str=(String)obj;
			str=isRight?str+rStr:rStr+str;
			this.putResult2Context(trimComma(str));//ContentUtil.putContent(vars,trimComma(str), content);
			return;
		}
		
		
		if(obj instanceof List){
			List list=(List)obj;
			if(params==null||params.length==0){
				StringBuilder sb=new StringBuilder();
				for(int i=0;i<list.size();i++){
					String str=(String)list.get(i);
					if(str!=null&&str.length()>0)sb.append(str).append(",");
				}
				if(sb.length()>0) sb.deleteCharAt(sb.length()-1);
				this.putResult2Context(sb.toString());//ContentUtil.putContent(vars, sb.toString(), content);
			}else{
				for(int i=0;i<list.size();i++){
					String str=(String)list.get(i);
					list.set(i,trimComma(isRight?str+rStr:rStr+str));
				}
				this.putResult2Context(list);//ContentUtil.putContent(vars, list, content);
			}
		}
	}
	private String trimComma(String str){
		if(str.startsWith(",")) str=str.substring(1);
		if(str.endsWith(",")) str=str.substring(0,str.length()-1);
		return str;
	}
	
	/**
	 * 字符串列表过滤
	 * @param content
	 */
	public void filter(String... params) {
		Object obj=this.inputData;
		
		if(obj==null){
			this.putResult2Context(null);//ContentUtil.putContent(vars, null, content);
		}
		
		ArrayList<String> ar=new ArrayList<String>();
		Pattern p=Pattern.compile(params[0]);
		
		
		if(obj instanceof String){
			if(!p.matcher((String)obj).find()){
				this.putResult2Context(null);//ContentUtil.putContent(vars, null, content);
			}
			return ;
		}
		
		
		if(obj instanceof List){
			List list=(List)obj;
			for(int i=0;i<list.size();i++){
				String str=(String)list.get(i);
				if(p.matcher(str).find()){
					ar.add(str);
				}
			}
			this.putResult2Context(ar);//ContentUtil.putContent(vars, ar, content);
		}
	}
	
	/**
	 * 数据抽取
	 * @param input
	 * @param params
	 * @param content
	 */
	public void extract(String... params){
		String reg=params[0];
		String placeS=params.length>1?params[1]:null;
		
		Pattern p=Pattern.compile(reg);
		
		Object obj=this.inputData;
		if(obj==null)
			this.putResult2Context(null);
			
		if(obj instanceof String){
			Matcher m=p.matcher(((String)obj));
			int index="all".equalsIgnoreCase(placeS)?-1:(placeS!=null?Integer.parseInt(placeS):1);
			StringBuilder sb=new StringBuilder();
			while(m.find()){
				if(index==-1){
					sb.append(m.group()).append(",");
				}else{
					if(index==1){
						sb.append(m.group()).append(",");
						break;
					}
					--index;
				}
			}
			if(sb.length()>1)sb.deleteCharAt(sb.length()-1);
			this.putResult2Context(sb.toString());//ContentUtil.putContent(vars, str, content);
			return;
		}
		
		if(obj instanceof List){
			List list=(List)obj;
			Object ar=null;
			
			if(!list.isEmpty()){
				if(placeS!=null){
					int index=Integer.parseInt(placeS);
					if(index>-1 && index<list.size()){
						String str=(String)list.get(index);
						Matcher m=p.matcher(str);
						if(m.find()){
							 str=m.group();
							 ar=str;
						}
					}
				}else{
					ar=new ArrayList<String>(list.size());
					for(int i=0;i<list.size();i++){
						String str=(String)list.get(i);
						Matcher m=p.matcher(str);
						if(m.find()){
							 str=m.group();
							 ((List)ar).add(i,str);
						}
					}
				}
			}
			this.putResult2Context(ar);//ContentUtil.putContent(vars, ar, content);
		}
	}

	public static void main(String[] args) throws Exception {
		InputStream is = new FileInputStream(new File("d:/test.txt"));
		String str = DataUtil.getInputStreamString(is, null);
		int startIndex = str.indexOf("__VIEWSTATE|") + "__VIEWSTATE|".length();
		str = str.substring(startIndex);
		str = str.substring(0, str.indexOf("|"));
		System.out.println(str);
	}

}
