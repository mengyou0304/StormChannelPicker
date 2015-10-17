/*
 * Copyright (c) 2015.
 * @author You.Meng
 */

package com.chinacache.robin.logic;

import com.chinacache.loganalyse.config.FAConstants;
import com.chinacache.loganalyse.model.LogType;
import com.chinacache.loganalyse.pick.service.LogPickerBuilderFactory;
import com.chinacache.loganalyse.pick.service.builder.LogEntityBuilder;
import com.chinacache.loganalyse.service.LineServicePick;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Map;

public class NLAKeyGenerator implements Serializable {

    private static final long serialVersionUID = 1L;

    private LogEntityBuilder logEntityBuilder;

    private static Long exceptionNum = 0L;
    private static Long allMessageNum = 0L;
    private static String NEED_GEN_CHANNEL = "NeedGenChannel";

    private static final String ERROR_USER = "0002";

    public void init() {
        logEntityBuilder = LogPickerBuilderFactory.logEntityBuilder;
    }

    public NLAResult execute(String messageLine) {
        if (messageLine == null || messageLine.trim().length() == 0) {
            return new NLAResult("EmptyMessage", ERROR_USER, "Empty", "");
        }

        allMessageNum++;
        String kafkaMessage = messageLine;
        try {
            String[] message = StringUtils.split(kafkaMessage, "\t");
            if (message.length != 2)
                return new NLAResult("TabNotTwo", ERROR_USER, "TabIsNotTwo", kafkaMessage);

            String[] fields = null;
            if (!(message[0].length() == 1 && Character.isUpperCase(message[0].charAt(0)))) {
                if (message[0].length() == 1 && message[0].charAt(0) == '@')
                    return new NLAResult("LogTypeError", ERROR_USER, "AtType", kafkaMessage);
                else return new NLAResult("LogTypeError", ERROR_USER, "TypeError", kafkaMessage);
            }

            fields = StringUtils.split(message[1], " ", 2);
            String deviceId = null;

            if (fields[0].equals(FAConstants.PATTERN_FC_ACCESS_CHANNEL)) {
                // 新的分频道后的FC日志文件的文件名deviceId没有用，真正的deviceId包含在文件的每一日志行中了
                // (第一个字段，该deviceId是分频道后程序添加的)
                deviceId = "0000000000";
            } else {
                deviceId = fields[0];
            }
            String nodeId = null;
            try {
                nodeId = deviceId.substring(0, 7);
            } catch (Exception e) {
                e.printStackTrace();
            }

            // Step2: 获取日志格式
            LogType type = logEntityBuilder.getLogType(message[0], deviceId);

            try {
                Map<String, String> map = LineServicePick.process(logEntityBuilder, fields[1], type);

                NLAResult res = new NLAResult(type.toString(), map.get("userId"), map.get("channelId"), message[1]);

                if (res.getChannelID() == null || res.getUserID() == null || res.getType() == null)
                    return new NLAResult("NullMember", ERROR_USER, "MemberNull", message[1]);

                String needGenChannel = map.get(NEED_GEN_CHANNEL);
                if (needGenChannel != null) {
                    String channel = res.getChannelID() + getPartitionFromSN(fields[0]);
                    res.setChannelID(channel);
                    res.setMessage(kafkaMessage);
                }
                return res;
            } catch (Exception e) {
                String channel = "ParseException" + getPartitionFromSN(fields[0]);
                String msg = e.getClass().getName() + " " + kafkaMessage;
                return new NLAResult("LogParseException", ERROR_USER, channel, msg);
            }

        } catch (Exception e) {
            exceptionNum++;
        }
        return new NLAResult("OtherException", ERROR_USER, "OtherException", messageLine);
    }

    public int getPartitionFromSN(String sn) {
        if (sn == null)
            return 0;
        else if (sn.trim().length() == 0)
            return 1;
        else {
            return 2 + (sn.trim().hashCode() & 0xF);
        }
    }

    public static void main(String[] args) {
        NLAKeyGenerator gen = new NLAKeyGenerator();
        String[] sns = { "010021H3dT", "01001743S3" };
        for (String sn : sns) {
            System.out.println(sn + ":" + gen.getPartitionFromSN(sn));
        }
    }
}
