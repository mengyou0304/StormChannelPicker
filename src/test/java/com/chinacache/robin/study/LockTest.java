/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin.study;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by robinmac on 15-8-21.
 */
public class LockTest extends Thread{
	String name;
	 Lock lock= new ReentrantLock();

	public LockTest(String name) {
		this.name = name;
	}

	public void m1() {
		lock.lock();
		System.out.println("[" + name + "]Go into M1");
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("[" + name + "]Out of M1");
		lock.unlock();
	}

	public void m2() {
		lock.lock();
		System.out.println("[" + name + "]Go into M2");
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("[" + name + "]Out of M2");
		lock.unlock();
	}

	public void m3() {
		synchronized (lock) {
			System.out.println("[" + name + "]Go into M3");
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("[" + name + "]Out of M3");
		}
	}

	public void m4() {
		synchronized (lock) {
			System.out.println("[" + name + "]Go into M4");
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("[" + name + "]Out of M4");
		}
	}

	@Override
	public void run() {
		m1();
		m2();
		m3();
		m4();
	}


	public static void main(String[] args) {
		LockTest lt = new LockTest("1");
		LockTest lt2 = new LockTest("2");
		lt.start();
		lt2.start();
	}


}
