package org.ops.rabbitmq.test;

import java.io.IOException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.Envelope;

public class ConsumerProxy extends DefaultConsumer {
  Reportor reportor;
  long threshold;
  long totalProcessed;

  public ConsumerProxy(Channel channel, Reportor reportor, int threshold) {

    super(channel);
    this.reportor = reportor;
    this.threshold = threshold;
    this.totalProcessed = 0;
  }
    
  @Override
  public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) throws IOException { 

    if (totalProcessed < threshold) {
      totalProcessed = reportor.addSuccess();
      if (totalProcessed <= threshold) {
        getChannel().basicAck(envelope.getDeliveryTag(), false);
      } else {
        totalProcessed = reportor.decreaseSuccess();
      }
    }
  }

  @Override
  public void handleShutdownSignal(String consumerTag,
      ShutdownSignalException sig) {
  }
}