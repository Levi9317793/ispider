package net.hubs1.spider.exception;

public class AccessDeniedException extends RuntimeException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public AccessDeniedException(String message){
		super(message);
	}
	
	public AccessDeniedException(String message,Throwable e){
		super(message,e);
	}
	
	public AccessDeniedException(Throwable e){
		super(e);
	}
}
