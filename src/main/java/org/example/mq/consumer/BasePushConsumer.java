package org.example.mq.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class BasePushConsumer {

    public static void main(String[] args) throws MQClientException {
        var consumer = new DefaultMQPushConsumer("consumerGroupDev");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.subscribe("devTopic", "*");
        consumer.setMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt msgExt : list) {
                    System.out.println("messageId:%s messageTag:%s messageBody:%s".formatted(msgExt.getMsgId(), msgExt.getTags(), new String(msgExt.getBody())));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("消费者启动成功.");
    }

}
