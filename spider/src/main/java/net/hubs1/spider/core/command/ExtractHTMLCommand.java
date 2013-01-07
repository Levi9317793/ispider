package net.hubs1.spider.core.command;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import net.hubs1.spider.core.BaseCommand;

public class ExtractHTMLCommand extends BaseCommand{
	
	private static Logger log=Logger.getLogger("ExtractHTMLCommand");
	
	public ExtractHTMLCommand(int lineNum, String commandName, String[] params,String[] vars) {
		super(lineNum, commandName, params,vars);
	}
	
	public void htmlgroup(String...params ){
		Object c=this.inputData;
		List<Element> eles=null;
		if(c!=null&&c instanceof List){
			eles=(List<Element>)c;
		}else{
			this.putResult2Context(null);
			return;
		}
		
		List<Elements> list=new ArrayList<Elements>();
		Iterator<Element> ite=eles.iterator();
		Element base=null;
		while(ite.hasNext()){
			Element e=ite.next();
			Elements es=new Elements();
			if(e.select(params[0]).size()>0){
				base=e;
				es.add(e);
			}else{
				if(base!=null) es.add(base);
				es.add(e);
			}
			list.add(es);
		}
		this.putResult2Context(list);
 	}
	
	/**
	 * html元素信息提取
	 * @param input
	 * @param params
	 * @param content
	 */
	public void htmlextract(String...params){
		String selectorRange=params[0];//所有带处理的节点
		String selectorCondition=params[1];//定位，找出符合条件的节点
		String nums=params[2];//在符合条件的节点基础上往前或者往后几个节点,可以为多个数字，用逗号风格。 负数表示往前
		
		String method=params.length>3?params[3]:null;//决定返回信息形式，如果有定义，返回指定的文本，否则返回节点自身
		
		
		Object c=this.inputData;
		
		if(c==null){
			this.putResult2Context( null);
			return;
		}
		
		Elements eles=null;
		
		
		
		if(c instanceof Elements){
			eles=(Elements)c;
		}
		
		if(c instanceof Element){
			eles=new Elements();
			eles.add((Element)c);
		}
		
		if(c instanceof String){
			eles=new Elements();
			eles.add(Jsoup.parse((String)c));
		}
		
		Elements range=eles.select(selectorRange);
		
		if(range.isEmpty()){
			this.putResult2Context( null);
			return;
		}
		
		/*查找出所有符合条件的节点*/
		String[] numss=nums.split(",");
		int[] indexs=new int[numss.length];
		for(int i=0;i<indexs.length;i++){
			indexs[0]=Integer.parseInt(numss[i]);
		}
		
		Elements resultE=new Elements();
		
		for(int index=0;index<range.size();index++){
			Element e=range.get(index);
			if(e.select(selectorCondition).size()>0){
				for(int i=0;i<indexs.length;i++){
					int p_index=indexs[i]+index;
					if(p_index>-1&&p_index<range.size()){
						resultE.add(range.get(p_index));
					}
				}
			}
		}
		
		
		if(method==null){
			this.putResult2Context(resultE);
		}else{
			Iterator<Element> ite=resultE.iterator();
			StringBuilder sb=new StringBuilder();
			while(ite.hasNext()){
				Element e=ite.next();
				String r=attr(e,method);
				if(r!=null) sb.append(r).append(",");
			}
			if(sb.length()>1) sb.deleteCharAt(sb.length()-1);
			this.putResult2Context(sb.toString());
		}
	}
	
	public void htmlfilter(String...params){
		
	}
	
	public void htmle(String...params){
		html(params);
	}
	
	public void html(String...params){
		Object c=this.inputData;
		if(c!=null){
			Element htmlDoc=null;
			
			if(c instanceof String){
				htmlDoc=Jsoup.parse((String)c);
			}else{
				if(c instanceof Element){
					htmlDoc=(Element)c;
				}
			}
			
			Elements es=!(c instanceof Elements)?toElements(htmlDoc):(Elements)c;
			if(es==null||es.isEmpty()){
				log.error("the document is null or empty,the input is:"+c);
				this.putResult2Context( null);
				return;
			}
			Object str= parse(es,params,commandName.equals("htmle"));
			
			if(str!=null && str instanceof List){
				List list=(List)str;
				if(list.size()==1)
					str=list.get(0);
				
				if(list.isEmpty()) str=null;
			}
			
			this.putResult2Context( str);
		
		}else{
			this.putResult2Context( null);
		}
	}
	
	
	private Elements toElements(Element... eles){
		Elements es=new Elements();
		for(Element e:eles){
			es.add(e);
		}
		return es;
	}

	
	/**
	 * 返回 List 或者 String。
	 * @param doc
	 * @param params
	 * @param contentMap
	 * @return
	 */
	private  Object parse(Elements doc,String[] params,boolean isE){
		String m=null;
		String attr=null;
		for(int i=0;i<params.length;i++){
			String param=params[i];
			int functionIndex=param.lastIndexOf("!");
			if(functionIndex>0){
				
				/*检查!是否在[]或者()中出现，如果是，则是表示jsoup自身的表达式*/
				String bp=param.substring(0,functionIndex);
				String ap=param.substring(functionIndex+1);
				boolean isc=false;
				/*这样的判断是不够准确的，在出现多个[]或（）时，判断可能是错误的*/
				if( (bp.lastIndexOf("[")>0&&ap.indexOf("]")>0) || (bp.lastIndexOf("(")>0&&ap.indexOf(")")>0)){
					isc=true;
				}
				if(!isc){
					m=param.substring(functionIndex+1);
					param=param.substring(0,functionIndex);
				}
			}
			if(i==params.length-1){
				if(m!=null&&m.contains(":")){
					attr=m.substring(m.indexOf(":")+1);
					m=m.substring(0,m.indexOf(":"));
				}else{
					int attrIndex=param.lastIndexOf(":");
					if(attrIndex>0){
//						
						/*检查:是否在[]或者()中出现，如果是，则是表示jsoup自身的表达式*/
						String bp=param.substring(0,attrIndex);
						String ap=param.substring(attrIndex+1);
						boolean isc=false;
						/*这样的判断是不够准确的，在出现多个[]或（）时，判断可能是错误的*/
						if( (bp.lastIndexOf("[")>0&&ap.indexOf("]")>0) || (bp.lastIndexOf("(")>0&&ap.indexOf(")")>0)){
							isc=true;
						}
						
						if(!isc){
							attr=param.substring(param.indexOf(":")+1);
							/*如果属性中带有括弧，表明是jsoup自身支持的方法，无需解析*/
							if( attr.contains("(")&&attr.contains(")")){
								attr=null;
							}else{
								param=param.substring(0,param.indexOf(":"));
							}
						}
					}
				}
			}
				
			doc=doc.select(param);
			if(!doc.isEmpty()&&m!=null){
				Element e=null;
				Elements ele=new Elements();
				if("last".equals(m)) 
						e=doc.last();
				    else if("first".equals(m)) 
				    		e=doc.first();
						else if("next".equals(m))
								e=doc.first().nextElementSibling();
							else if(m.matches("^\\d+(\\w+)?")){
									if(m.endsWith("next")){
										e=doc.get(Integer.parseInt(m.substring(0,m.indexOf("next")))).nextElementSibling();
									}
									if(m.endsWith("nextAll")){
										int index=Integer.parseInt(m.substring(0,m.indexOf("nextAll")));
										for(int start=index;start<doc.size();start++){
											ele.add(doc.get(start));
										}
									}
									if(m.matches("\\d+$")) 
										e=doc.get(Integer.parseInt(m));
								}
				if(e==null&&ele.isEmpty()) break;
				if(e!=null) ele.add(e);
				doc=ele;
				m=null;
			}
			if(doc.isEmpty()) break;
		}
		
		if(isE) return doc;
		
		if(attr==null) attr="text";
		
		return attr(doc,attr);
	}
	
	
	private  Object attr(Elements es,String attrStr){
		if("size".equalsIgnoreCase(attrStr)){
			if(es==null||es.isEmpty()) return "0";
			return String.valueOf(es.size());
		}
		
		if(es==null||es.isEmpty()) return null;
		
		List<String> list=new ArrayList<String>();
		Iterator<Element> ite=es.iterator();
		while(ite.hasNext()){
			Element e=ite.next();
			String r=attr(e,attrStr);
			if(r!=null)
				list.add(r);
		}
		return list;
	}
	
	private  String attr(Element e,String attrStr){
		String r=null;
		if("text".equals(attrStr))
			r=e.text();
		else if("ownerText".equals(attrStr))
			r=e.ownText();
		else if("html".equals(attrStr))
			r=e.html();
		else
			r=e.attr(attrStr);
		return r;
	}
}
