package com.chinacache.robin.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Dispatches {@link Event}s in a separate thread. Currently only single thread
 * does that.
 * 
 * @author zexing.hu
 *
 * add event of putToKakfa process
 * @you.meng
 */
@SuppressWarnings("rawtypes")
public class AsyncDispatcher {

	public static interface EventHandler<T extends Event> {
		void handle(T event);
	}

	private static final Logger LOG = LoggerFactory
			.getLogger(AsyncDispatcher.class);

	private BlockingQueue<Event> eventQueue;

	private Map<Class<? extends Enum>, EventHandler> eventDispatchers;

	private Thread eventDispatchThread;

	public AsyncDispatcher() {
		eventQueue = new LinkedBlockingQueue<Event>();
		eventDispatchers = new HashMap<Class<? extends Enum>, EventHandler>();
	}

	private Runnable createThread() {
		return new Runnable() {
			@Override
			public void run() {
				while (!Thread.currentThread().isInterrupted()) {
					try {
						Event event = eventQueue.take();
						LOG.info("Get Event={}", event);
						dispatch(event);
					} catch (InterruptedException e) {
						LOG.warn("AsyncDispatcher thread interrupted", e);
						return;
					}
				}
			}
		};
	}

	@SuppressWarnings("unchecked")
	private void dispatch(Event event) {
		Class<? extends Enum> clazz = event.getType().getClass();
		try {
			EventHandler handler = eventDispatchers.get(clazz);
			if (handler == null) {
				LOG.error("No handler for event={}", event.toString());
			} else {
				LOG.info("Handle Event={} by Handler {}", event, handler);
				handler.handle(event);
			}
		} catch (Throwable t) {
			LOG.error("Error in dispatcher thread", t);
		}
	}

	@SuppressWarnings("unchecked")
	public void register(Class<? extends Enum> clazz, EventHandler handler) {
		EventHandler<Event> eventHandler = (EventHandler<Event>) eventDispatchers
				.get(clazz);
		if (eventHandler == null) {
			LOG.info("Register EventHandler={} for Event={}", handler
					.getClass().getName(), clazz.getName());
		} else {
			LOG.info(
					"Already exist handler, use new EventHandler={} for Event={}",
					handler.getClass().getName(), clazz.getName());
		}
		eventDispatchers.put(clazz, handler);
	}

	public void start() {
		eventDispatchThread = new Thread(createThread());
		eventDispatchThread.setName("AsyncDispatcher event handler");
		eventDispatchThread.start();
	}

	public void stop() {
		if (eventDispatchThread != null) {
			eventDispatchThread.interrupt();
			try {
				eventDispatchThread.join();
			} catch (InterruptedException ie) {
				LOG.warn("Interrupted Exception while stopping", ie);
			}
		}
	}

	public void putEvent(Event event) {
		try {
			eventQueue.put(event);
			LOG.info("Put event={}", event);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
