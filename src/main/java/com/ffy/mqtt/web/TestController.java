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

    @PostMapping("send")
    public Message sendMsg(@RequestBody Message message) {
        log.info("sendMsg:" + message.getMessageId());
        DefaultFuture future = synMqttSender.sendMessage(message);
        try {
            return future.get();
        } catch (Exception e) {
            log.error("message_id:" + message.getMessageId());
            throw e;
        }

    }
}
