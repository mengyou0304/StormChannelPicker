package com.chinacache.robin.service;

/**
 * Parent class of all the events. All events extend this class.
 * 
 * @author zexing.hu
 *
 */
public class AbstractEvent<EventType extends Enum<EventType>> implements
		Event<EventType> {

	private final EventType type;

	private final long timestamp;

	protected AbstractEvent(EventType type) {
		this.type = type;
		timestamp = -1L;
	}

	protected AbstractEvent(EventType type, long timestamp) {
		this.type = type;
		this.timestamp = timestamp;
	}

	@Override
	public EventType getType() {
		return type;
	}

	@Override
	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public String toString() {
		return "EventType:" + getType();
	}
}
