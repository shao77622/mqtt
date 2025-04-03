package com.ffy.mqtt.web;

import com.ffy.mqtt.model.Message;
import com.ffy.mqtt.mqtt.SynMqttSender;
import com.ffy.mqtt.util.DefaultFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("mqtt")
@Slf4j
public class TestController {
    @Autowired
    SynMqttSender synMqttSender;

    @PostMapping("sendMsgUpdate")
    public Message sendMsgUpdate(@RequestBody Message message) {
        synMqttSender.sendMessageUpdate(message);
        return message;
    }

    @PostMapping("sendMsgRrpc")
    public Message sendMsgRrpc(@RequestBody Message message) {
        DefaultFuture future = synMqttSender.sendMessageRrpc(message);
        try {
            return future.get();
        } catch (Exception e) {
            log.error("message_id:" + message.getMessageId());
            throw e;
        }
    }
}
