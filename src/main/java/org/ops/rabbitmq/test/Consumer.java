package org.ops.rabbitmq.test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;

public class Consumer {

  private AtomicBoolean exit = new AtomicBoolean(false);

  private int maxWorkers = 64;
  private int numOfMessages = 0;
  private String server;
  private int port;
  private String exchange;
  private String routingKey;
  private String queue;
  private boolean durable;

  public Consumer(int numOfMessage, int maxWorkers,
      String server, int port, String exchange, String routingKey,
      String queue, boolean durable) {

    this.numOfMessages = numOfMessage;
    this.maxWorkers = maxWorkers;
    this.server = server;
    this.port = port;
    this.exchange = exchange;
    this.routingKey = routingKey;
    this.queue = queue;
    this.durable = durable;

  }

  public void execute() {

    try {
      Reportor reportor = new Reportor();
      
      DefaultConsumer[] consumers = new DefaultConsumer[maxWorkers];

      ExecutorService executorService = Executors
          .newFixedThreadPool(consumers.length);

      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost(server);
      factory.setPort(port);

      Connection connection = factory.newConnection(executorService);

      for (int i = 0; i < maxWorkers; ++i) {
        Channel channel = connection.createChannel();

        channel.basicQos(1);

        // creating routing
        channel.exchangeDeclare(exchange, "direct");
        channel.queueDeclare(queue, durable, false, false, null);
        channel.queueBind(queue, exchange, routingKey);

        ConsumerProxy consumerProxy = new ConsumerProxy(channel, reportor, numOfMessages);

        consumers[i] = consumerProxy;
      }

      long elapse = 0;
      long start = System.currentTimeMillis();

      for (int i = 0; i < maxWorkers; ++i) {
        consumers[i].getChannel().basicConsume(queue, false, consumers[i]);
      }

      try {
        while (!exit.get() && reportor.getTotal() < numOfMessages) {
          // Waiting until stopping or connection dropped
          if (!exit.get()) {
            Thread.sleep(0);
          }
        }

        elapse = System.currentTimeMillis() - start;

        for (int i = 0; i < consumers.length; ++i) {
          consumers[i].getChannel().close();
        }

        Thread.sleep(1000);

        System.out.println("shutdown workers");

        try {
          executorService.shutdown();
          executorService.awaitTermination(86400, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

      } catch (InterruptedException e) {
        e.printStackTrace();
      } finally {
        connection.close();
      }
      
      System.out.println(String.format(
          "Complete : rps=%f (in %d ms), success=%d, failure=%d",
          (numOfMessages * 1000.0) / elapse, elapse, reportor.getSuccess(),
          reportor.getFailure()));

    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("Cannot connect to RabbitMQ server");
    }
  }

}
