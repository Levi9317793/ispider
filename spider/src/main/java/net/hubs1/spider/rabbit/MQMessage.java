package net.hubs1.spider.rabbit;

public class MQMessage<T> {
	
	public static enum Status{STOP,RECEIVE};
	
	private long responseTag = -1;
	private Status status=Status.RECEIVE;
	private T message = null;
	
	public MQMessage(long tag,T kv){
		this.responseTag=tag;
		this.message=kv;
	}
	
	public MQMessage(){}
	

	public T getMessage() {
		return message;
	}

	public void setMessage(T message) {
		this.message = message;
	}

	/**
	 * 表明当前是否还需要继续获取消息。超过2分钟，无法获取消息，则认为下载任务已结束。
	 * @return
	 */
	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public long getResponseTag() {
		return responseTag;
	}

	public void setResponseTag(long responseTag) {
		this.responseTag = responseTag;
	}

}
