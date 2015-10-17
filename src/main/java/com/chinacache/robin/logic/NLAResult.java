/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin.logic;

import java.io.Serializable;

public class NLAResult implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String userID;
	private String type;
	private String channelID;
	private String message;

	public NLAResult(String type, String userId, String channelId,
	                 String _message) {
		this.type = type;
		this.userID = userId;
		this.channelID = channelId;
		this.message = _message;
	}

	public String getUserID() {
		return userID;
	}

	public void setUserID(String userID) {
		this.userID = userID;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getChannelID() {
		return channelID;
	}

	public void setChannelID(String channelID) {
		this.channelID = channelID;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public String toString() {
		String line = "";
		line += this.type + ",";
		line += this.userID + ",";
		line += this.channelID + ",";
		line += this.message;
		return line;
	}

}
