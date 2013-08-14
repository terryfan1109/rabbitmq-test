package org.ops.rabbitmq.test;

public class Message {
  public String exchange;
  public String routingKey;
  public String queue;
  public String body;

  public Message(String exchange, String routingKey, String body) {
    
    this.exchange = exchange;
    this.routingKey = routingKey;
    this.body = body;
  }
}