package com.chinacache.robin.util.basic;

import java.util.ArrayList;
import java.util.Random;

public class Sorter {

	private static int partition(ArrayList<Entry> list, int st, int ed) {
		int j = st - 1;
		int x = list.get(ed).getWeight();
		for (int i = st; i < ed; i++)
			if (list.get(i).getWeight() > x)
				swap(list, ++j, i);
		swap(list, ++j, ed);
		return j;
	}

	private static void swap(ArrayList<Entry> list, int i, int i2) {
		Entry tmp = list.get(i);
		list.set(i, list.get(i2));
		list.set(i2, tmp);
	}

	public static void qsort(ArrayList<Entry> list, int st, int ed) {
		if (st > ed)
			return;
		int i = partition(list, st, ed);
		qsort(list, st, i - 1);
		qsort(list, i + 1, ed);
	}

	public static void main(String[] args) {
		Random r = new Random();
		ArrayList<Entry> list = new ArrayList<Entry>();
		for (int i = 0; i < 30000; i++) {
			Entry e = new Entry(r.nextInt(3000), "num" + i);
			list.add(e);
		}
		Sorter.qsort(list, 0, list.size() - 1);
		for (Entry e : list)
			System.out.println(e);
	}

}
