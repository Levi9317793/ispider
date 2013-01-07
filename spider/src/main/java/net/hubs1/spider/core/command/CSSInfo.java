package net.hubs1.spider.core.command;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CSSInfo implements Comparable<CSSInfo>{
	
	private String cssName=null;
	private String value=null;
	private int zindex=0;
	private int left=0;
	private int fontWidthSize=0;
//	private int pading=0;
//	private int merge=0;
	
	@Override
	public int compareTo(CSSInfo o) {
		if(this.zindex>o.getZindex()&&this.left==o.getLeft())
			return 1;
		
		return this.left-o.getLeft();
	}

	public String getCssName() {
		return cssName;
	}

	public void setCssName(String cssName) {
		this.cssName = cssName;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public int getZindex() {
		return zindex;
	}

	public void setZindex(int zindex) {
		this.zindex = zindex;
	}

	public int getLeft() {
		return left;
	}

	public void setLeft(int left) {
		this.left = left;
	}

	public int getFontWidthSize() {
		return fontWidthSize;
	}

	public void setFontWidthSize(int fontWidthSize) {
		this.fontWidthSize = fontWidthSize;
	}
	
	public static Map<String,CSSInfo> fromString(String str){
		Map<String,CSSInfo> map=new HashMap<String,CSSInfo>();
		
		str=str.replaceAll("\r\n", "");
		String reg="[-. _\\w]\\{[][^\\{\\}]\\}";
		Pattern p=Pattern.compile(reg);
		
		Pattern numberP=Pattern.compile("\\d+");
		
		Matcher m=p.matcher(str);
		
		while(m.find()){
			String css=m.group();
			String name=css.substring(0,css.indexOf("{")).toString();
			
			int start=css.indexOf("z-index:")+"z-index:".length();
			String zindex=null;
			String left=null;
			if(start>-1) zindex=css.substring(start,css.indexOf(";", start));
			
			start=css.indexOf("left:")+"left:".length();
			if(start>-1)left=css.substring(start,css.indexOf(";", start));
			
			CSSInfo cssinfo=new CSSInfo();
			cssinfo.setCssName(name);
			
			Matcher lm=numberP.matcher(zindex);
			if(lm.find()){
				zindex=lm.group();
				cssinfo.setZindex(Integer.parseInt(zindex));
			}
			
			lm=numberP.matcher(left);
			
			if(lm.find()){
				left=lm.group();
				cssinfo.setLeft(Integer.parseInt(left));
			}
		}
		
		return map;
	}

}
