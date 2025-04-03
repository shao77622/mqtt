package com.ffy.mqtt.mqtt;

import cn.hutool.json.JSONUtil;
import com.ffy.mqtt.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.support.DefaultPahoMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.annotation.Resource;

/**
 * MQTT配置，消费者
 */
@Configuration
@Slf4j
public class MqttReceiverConfig {
    /**
     * 订阅的bean名称
     */
    public static final String CHANNEL_NAME_IN = "mqttInboundChannel";

    // 客户端与服务器之间的连接意外中断，服务器将发布客户端的“遗嘱”消息
    private static final byte[] WILL_DATA;

    static {
        WILL_DATA = "offline".getBytes();
    }

    @Bean("updateTaskExecutor")
    public ThreadPoolTaskExecutor updateTaskExecutor() {
        //实时性不高，qps高，可进队列
        ThreadPoolTaskExecutor pool = new ThreadPoolTaskExecutor();
        pool.setThreadNamePrefix("updateTaskExecutor-");
        // 线程池维护线程的最少数量
        pool.setCorePoolSize(20);
        // 线程池维护线程的最大数量
        pool.setMaxPoolSize(100);
        pool.setKeepAliveSeconds(60);
        pool.setQueueCapacity(500);
        return pool;
    }

    @Bean("rrpcTaskExecutor")
    public ThreadPoolTaskExecutor rrpcTaskExecutor() {
        //实时性高，qps低，尽量不进队列
        ThreadPoolTaskExecutor pool = new ThreadPoolTaskExecutor();
        pool.setThreadNamePrefix("rrpcTaskExecutor-");
        // 线程池维护线程的最少数量
        pool.setCorePoolSize(5);
        // 线程池维护线程的最大数量
        pool.setMaxPoolSize(200);
        pool.setKeepAliveSeconds(60);
        pool.setQueueCapacity(20);
        return pool;
    }

    @Resource
    private ThreadPoolTaskExecutor updateTaskExecutor;
    @Resource
    private ThreadPoolTaskExecutor rrpcTaskExecutor;


    @Value("${mqtt.username}")
    private String username;

    @Value("${mqtt.password}")
    private String password;

    @Value("${mqtt.url}")
    private String url;

    @Value("${mqtt.receiver.clientId}")
    private String clientId;

    @Value("${mqtt.receiver.updateTopic}")
    private String updateTopic;

    @Value("${mqtt.receiver.rrpcResponseTopic}")
    private String rrpcResponseTopic;

    /**
     * MQTT连接器选项
     */
    @Bean
    public MqttConnectOptions getReceiverMqttConnectOptions() {
        MqttConnectOptions options = new MqttConnectOptions();
        // 设置连接的用户名
        if (!username.trim().equals("")) {
            options.setUserName(username);
        }
        // 设置连接的密码
        options.setPassword(password.toCharArray());
        // 设置连接的地址
        options.setServerURIs(StringUtils.split(url, ","));
        // 设置超时时间 单位为秒
        options.setConnectionTimeout(10);
        // 设置会话心跳时间 单位为秒 服务器会每隔1.5*20秒的时间向客户端发送心跳判断客户端是否在线
        // 但这个方法并没有重连的机制
        options.setKeepAliveInterval(20);
        return options;
    }

    /**
     * MQTT客户端
     */
    @Bean
    public MqttPahoClientFactory receiverMqttClientFactory() {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        factory.setConnectionOptions(getReceiverMqttConnectOptions());
        return factory;
    }

    /**
     * MQTT信息通道（消费者）
     */
    @Bean(name = CHANNEL_NAME_IN)
    public MessageChannel mqttInboundChannel() {
        return new DirectChannel();
//        return new ExecutorChannel(Executors.newCachedThreadPool());
    }

    /**
     * MQTT消息订阅绑定（消费者）
     */
    @Bean
    public MessageProducer inbound() {
        // 可以同时消费（订阅）多个Topic
        MqttPahoMessageDrivenChannelAdapter adapter =
                new MqttPahoMessageDrivenChannelAdapter(
                        clientId, receiverMqttClientFactory(),
                        new String[]{updateTopic, rrpcResponseTopic});
        adapter.setCompletionTimeout(5000);
        adapter.setConverter(new DefaultPahoMessageConverter());
        adapter.setQos(new int[]{0, 1});
        // 设置订阅通道
        adapter.setOutputChannel(mqttInboundChannel());
        return adapter;
    }

    @Autowired
    MqttResHandler mqttResHandler;

    /**
     * MQTT消息处理器（消费者）
     */
    @Bean
    @ServiceActivator(inputChannel = CHANNEL_NAME_IN)
    public MessageHandler handler() {
        return new MessageHandler() {
            @Override
            public void handleMessage(Message<?> message) throws MessagingException {
                String topic = message.getHeaders().get("mqtt_receivedTopic").toString();

                if (topic.equals(Constant.MQTT_TOPIC_UPDATE)) {
                    updateTaskExecutor.submit(new Runnable() {
                        @Override
                        public void run() {
//                            try {
//                                Thread.sleep(10000);
//                            } catch (InterruptedException e) {
//                                throw new RuntimeException(e);
//                            }
                            String msg = message.getPayload().toString();
//                            mqttResHandler.deal(JSONUtil.toBean(msg, com.ffy.mqtt.model.Message.class));
                            log.info("\n--------------------START-------------------\n" +
                                    "接收到订阅消息:\ntopic:" + topic + "\nmessage:" + msg +
                                    "\n---------------------END--------------------");
                        }
                    });

                } else if (topic.startsWith(Constant.MQTT_TOPIC_RRPC_RES)) {
                    rrpcTaskExecutor.submit(new Runnable() {
                        @Override
                        public void run() {
//                            try {
//                                Thread.sleep(2000);
//                            } catch (InterruptedException e) {
//                                throw new RuntimeException(e);
//                            }
                            String msg = message.getPayload().toString();
                            String messageId = topic.substring(topic.lastIndexOf("/") + 1);
                            com.ffy.mqtt.model.Message msg1 = new com.ffy.mqtt.model.Message();
                            msg1.setMessageId(Long.valueOf(messageId));
                            msg1.setPayLoad(msg);
                            mqttResHandler.deal(msg1);
                            log.info("\n--------------------START-------------------\n" +
                                    "接收到订阅消息:\ntopic:" + topic + "\nmessage:" + msg +
                                    "\n---------------------END--------------------");
                        }
                    });
                }
            }
        };
    }
}

