package org.example;

import javax.jms.*;
import javax.swing.plaf.synth.SynthTextAreaUI;

import org.apache.activemq.ActiveMQConnectionFactory;

import java.lang.reflect.Array;
import java.time.LocalTime;


/**
 * Hello world!
 */
public class Main {
    public static void main(String[] args) throws Exception {
//        thread(new HelloWorldProducer(), false);
//        thread(new HelloWorldProducer(), false);
//        thread(new HelloWorldConsumer(), false);
//        Thread.sleep(1000);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldProducer(), false);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldProducer(), false);
//        Thread.sleep(1000);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldProducer(), false);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldProducer(), false);
//        thread(new HelloWorldProducer(), false);
//        Thread.sleep(1000);
//        thread(new HelloWorldProducer(), false);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldProducer(), false);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldProducer(), false);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldProducer(), false);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldConsumer(), false);
//        thread(new HelloWorldProducer(), false);

        int numMessages = 1;
        try {
            numMessages = Integer.parseInt(args[0]);

        } catch (Error e) {
            System.out.println("No argument provided, defaulting to 1 message");
        }
        System.out.print("Number of messages: " + numMessages + "\n");


        while(true) {
            for (int i = 0; i < numMessages; i++) {
                thread(new HelloWorldProducer(), false);
                thread(new HelloWorldConsumer(), false);
                Thread.sleep(10);
            }
            Thread.sleep(1000);
        }




    }

    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }

    public static class HelloWorldProducer implements Runnable {
        public void run() {
            try {
                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");

                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                // Create the destination (Topic or Queue)
                Destination destination = session.createQueue("TEST.FOO");

                // Create a MessageProducer from the Session to the Topic or Queue
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                // Create a messages
//                String text = "Hello world! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
                LocalTime time = LocalTime.now();
                String text = "{ time " + time.getNano() + " address: 317 Castlereagh St, Sydney NSW 2000, Australia, phone: +61 2 9264 3000, email:gortonator@gortgort.com, website: https://www.gortgort.com/}";
                TextMessage message = session.createTextMessage(text);

                // Tell the producer to send the message
                System.out.println("Sent message at time: " + time + " : " + message.hashCode() + " : " + Thread.currentThread().getName());

                producer.send(message);

                // Clean up
                session.close();
                connection.close();
            }
            catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }
    }

    public static class HelloWorldConsumer implements Runnable, ExceptionListener {
        public void run() {
            try {
                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("vm://localhost");

                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();

                connection.setExceptionListener(this);

                // Create a Session
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                // Create the destination (Topic or Queue)
                Destination destination = session.createQueue("TEST.FOO");

                // Create a MessageConsumer from the Session to the Topic or Queue
                MessageConsumer consumer = session.createConsumer(destination);

                // Wait for a message
                Message message = consumer.receive(1000);

                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    String text = textMessage.getText();
                    LocalTime time = LocalTime.now();
                    System.out.println("Received at time " + time);
                    System.out.println("Received: " + text);

                    String[] split = text.split(" ");
                    int sentTime = Integer.parseInt(split[2]);
                    System.out.println("Messgage Latency: " + (time.getNano() - sentTime) + " nanoseconds");
                } else {
                    System.out.println("Received: " + message);
                }

                consumer.close();
                session.close();
                connection.close();
            } catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }

        public synchronized void onException(JMSException ex) {
            System.out.println("JMS Exception occured.  Shutting down client.");
        }
    }
}