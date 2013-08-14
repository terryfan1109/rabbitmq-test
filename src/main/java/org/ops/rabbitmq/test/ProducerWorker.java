package org.ops.rabbitmq.test;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

public class ProducerWorker implements Runnable {
  AtomicBoolean exit = new AtomicBoolean(false);
  Channel channel;
  BlockingQueue<Message> queue;
  Reportor reportor;

  public ProducerWorker(Channel channel, BlockingQueue<Message> queue,
      Reportor reportor) {

    this.channel = channel;
    this.queue = queue;
    this.reportor = reportor;
  }

  public void stop() {

    exit.set(true);
  }

  @Override
  public void run() {

    try {
      while (!exit.get()) {
        Message msg = queue.poll(1, TimeUnit.SECONDS);
        while (null != msg) {
          try {
            channel.basicPublish(msg.exchange, msg.routingKey,
                MessageProperties.PERSISTENT_BASIC, msg.body.getBytes("UTF-8"));
            reportor.addSuccess();
          } catch (IOException e) {
            e.printStackTrace();
            reportor.addFailure();
          }
          msg = queue.poll(1, TimeUnit.SECONDS);
        }
      }
    } catch (InterruptedException e) {
      // ...
      e.printStackTrace();
    }
  }

}