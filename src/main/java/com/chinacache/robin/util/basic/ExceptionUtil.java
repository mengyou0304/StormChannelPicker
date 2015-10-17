package com.chinacache.robin.util.basic;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ExceptionUtil {
	public static SimpleDateFormat sdf = new SimpleDateFormat(
			"yyyy-MM-dd:hh-mm-ss");

	public static String getExcetpionInfo(Exception e, String key) {
		String infoline = "from " + key + " @time " + sdf.format(new Date()) + "\n";

		if (e != null) {
			infoline += "message: " + e.getMessage() + ":" + e.getCause()
					+ e.getLocalizedMessage() + "\n";
			StackTraceElement[] stes = e.getStackTrace();
			for (StackTraceElement ste : stes) {
				infoline += ste.toString() + "\n";
			}
			Throwable[] ts = e.getSuppressed();
			for (Throwable tr : ts) {
				infoline += tr + "\n";
			}
		}
		return infoline;
	}

}
