import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;

public class ConsumerThread implements Runnable {
    private final static String QUEUE_NAME = "myQueue";
    private final Connection connection;
    private final ConcurrentHashMap<String, String> map;

    public ConsumerThread(Connection connection, ConcurrentHashMap<String, String> map) {
        this.connection = connection;
        this.map = map;
    }
    @Override
    public void run() {
        try {
            Channel channel = this.connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            // max one message per receiver
            channel.basicQos(1);
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                // keeps a record of the individual lift rides for each skier in a hash map.
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                System.out.println( "Callback thread ID = " + Thread.currentThread().getId() +
                        " Received '" + message + "'");

                Gson gson = new Gson();
                String[] parts = message.split("!");
                String skierID = String.valueOf(parts[0]);
                String jsonString = gson.toJson(parts[1]);
                this.map.put(skierID, jsonString);
            };
            // process messages
            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
