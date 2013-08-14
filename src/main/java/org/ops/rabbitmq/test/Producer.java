package org.ops.rabbitmq.test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;

import org.apache.commons.codec.binary.Base64;

public class Producer {

  private int initWorkers = 0;
  private int maxWorkers = 0;
  private int numOfMessages = 0;
  private String server;
  private int port;
  private String exchange;
  private String routingKey;
  private String queue;
  private boolean durable;

  public Producer(int numOfMessage, int initWorkers, int maxWorkers,
      String server, int port, String exchange, String routingKey, String queue, boolean durable) {

    this.numOfMessages = numOfMessage;
    this.initWorkers = initWorkers;
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
      long elapse = 0;

      {
        ProducerWorker[] workers = new ProducerWorker[maxWorkers];

        ArrayBlockingQueue<Message> messageQueue = new ArrayBlockingQueue<Message>(
            workers.length);

        ArrayBlockingQueue<Runnable> workerQueue = new ArrayBlockingQueue<Runnable>(
            workers.length);

        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(initWorkers,
            workers.length, 60, TimeUnit.SECONDS, workerQueue);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(server);
        factory.setPort(port);

        Connection connection = factory.newConnection();

        for (int i = 0; i < maxWorkers; ++i) {
          Channel channel = connection.createChannel();
          
          // creating routing
          channel.exchangeDeclare(exchange, "direct");
          channel.queueDeclare(queue, durable, false, false, null);
          channel.queueBind(queue, exchange, routingKey);

          ProducerWorker worker = new ProducerWorker(channel, messageQueue,
              reportor);
          workers[i] = worker;
        }

        for (int i = 0; i < workers.length; ++i) {
          threadPool.submit(workers[i]);
        }

        long start = System.currentTimeMillis();
        Random randGen = new Random();
        byte[] randMessage = new byte[2 * 1024];

        for (int i = 0; i < numOfMessages; ++i) {
          randGen.nextBytes(randMessage);
          String msg = Base64.encodeBase64URLSafeString(randMessage);
          try {
            messageQueue.put(new Message(exchange, routingKey, msg));
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }

        System.out.println("signal workers to stop");

        for (int i = 0; i < workers.length; ++i) {
          workers[i].stop();
        }

        System.out.println("shutdown workers");

        try {
          threadPool.shutdown();
          threadPool.awaitTermination(86400, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          // ...
        } finally {
          connection.close();
        }

        elapse = System.currentTimeMillis() - start;
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
