package com.ffy.mqtt.mqtt;

import cn.hutool.json.JSONUtil;
import com.ffy.mqtt.constant.Constant;
import com.ffy.mqtt.model.Message;
import com.ffy.mqtt.util.DefaultFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Component
@Slf4j
public class SynMqttSender {
    @Autowired
    IMqttSender iMqttSender;


    public void sendMessageUpdate(Message msg) {
        Long msgId = DefaultFuture.generateId();
        msg.setMessageId(msgId);
        iMqttSender.sendToMqtt(Constant.MQTT_TOPIC_UPDATE, 0, JSONUtil.toJsonStr(msg));
//        log.info("*****sendMessage" + msg.getMessageId());
    }

    public DefaultFuture sendMessageRrpc(Message msg) {
        Long msgId = DefaultFuture.generateId();
        msg.setMessageId(msgId);
        DefaultFuture future = new DefaultFuture(msg.getMessageId(), 3);
        iMqttSender.sendToMqtt(Constant.MQTT_TOPIC_RRPC_REQ + msgId, 1, JSONUtil.toJsonStr(msg));
//        log.info("*****sendMessage" + msg.getMessageId());
        return future;
    }

}
