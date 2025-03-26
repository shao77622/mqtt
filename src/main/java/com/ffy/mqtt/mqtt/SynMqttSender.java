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
    public DefaultFuture sendMessage(Message msg)  {
        Long msgId = DefaultFuture.generateId();
        msg.setMessageId(msgId);
        DefaultFuture future = new DefaultFuture(msg.getMessageId(),3);
        iMqttSender.sendToMqtt(Constant.MQTT_TOPIC_REQ,0, JSONUtil.toJsonStr(msg));
//        log.info("*****sendMessage" + msg.getMessageId());
        return future;
    }
}
