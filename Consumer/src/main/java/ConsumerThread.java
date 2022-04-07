import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.nio.charset.StandardCharsets;;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import com.google.gson.Gson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class ConsumerThread implements Runnable {
    private final static String QUEUE_NAME = "queue1";
    private final Connection connection;
    private final ConcurrentHashMap<String, String> map;
    private final static String EXCHANGE_NAME = "my-fanout-exchange";

    public ConsumerThread(Connection connection, ConcurrentHashMap<String, String> map) {
        this.connection = connection;
        this.map = map;
    }
    @Override
    public void run() {
        try {
            Channel channel = this.connection.createChannel();
            // TODO:declare fanout?
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "");
            // max one message per receiver
            channel.basicQos(1);
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            Map<String, Integer> dayCounter = new HashMap();
            Set<String> uniqueDays = new HashSet<>();
            Map<String, Map<String, Integer>> verticalCounter = new HashMap();
            Map<String, Map<String, List>> liftsByDayBySkier = new HashMap();

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                // keeps a record of the individual lift rides for each skier in a hash map.
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
//                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                System.out.println( "Callback thread ID = " + Thread.currentThread().getId() +
                        " Received '" + message + "'");

                // store skier to redis
                Gson gson = new Gson();
                String[] parts = message.split("!");
                String resortID = String.valueOf(parts[0]);
                String seasonID = String.valueOf(parts[1]);
                String dayID = String.valueOf(parts[2]);
                String skierID = String.valueOf(parts[3]);
                String jsonString = gson.toJson(parts[4]);
                int startIdx = jsonString.lastIndexOf(':');
                int endIdx = jsonString.lastIndexOf('}');
                String liftID = jsonString.substring(startIdx + 1, endIdx);

                String portNumber = "54.189.70.118"; // todo:redis
                JedisPool pool = new JedisPool(new JedisPoolConfig(),
                        portNumber, 6379, Protocol.DEFAULT_TIMEOUT);

                Jedis redis1 = null;
                Jedis redis2 = null;

//                RedisConnection db = new RedisConnection();
//                Jedis jedis1 = db.getJedis();
//                Jedis jedis2 = db.getJedis();

                try {
                    redis1 = pool.getResource();
                    redis2 = pool.getResource();
                    // For skier N, how many days have they skied this season?
                    if (dayCounter.containsKey(skierID)) {
                        if (!uniqueDays.contains(dayID)) {
                            dayCounter.replace(skierID, dayCounter.get(skierID) + 1);
                            uniqueDays.add(dayID);
                        }
                    } else {
                        dayCounter.put(skierID, 1);
                    }
                    redis1.set(dayID, String.valueOf(dayCounter.get(skierID)));

                    // For skier N, what are the vertical totals for each ski day?
                    int vertical = Integer.valueOf(liftID) * 10;
                    if (verticalCounter.containsKey(skierID)) {
                        Map<String, Integer> innerMap = verticalCounter.get(skierID);
                        if (!innerMap.containsKey(dayID)) {
                            innerMap.replace(dayID, innerMap.get(dayID) + vertical);
                        } else {
                            innerMap.put(dayID, vertical);
                        }
                    } else {
                        verticalCounter.put(skierID, new HashMap<>());
                    }
                    redis2.set(skierID, verticalCounter.get(skierID).toString());

                    // For skier N, show me the lifts they rode on each ski day
                    if (liftsByDayBySkier.containsKey(skierID)) {
                        Map<String, List> innerMap = liftsByDayBySkier.get(skierID);
                        if (!innerMap.containsKey(dayID)) {
                            innerMap.get(dayID).add(liftID);
                        } else {
                            innerMap.put(dayID, new ArrayList());
                        }
                    } else {
                        liftsByDayBySkier.put(skierID, new HashMap<>());
                    }

                    this.map.put(skierID, jsonString);
                } catch (JedisConnectionException e) {
                        if (redis1 != null ) {
                            pool.returnBrokenResource(redis1);
                            redis1 = null;
                        }
                        if (redis2 != null ) {
                            pool.returnBrokenResource(redis2);
                            redis2= null;
                        }
                        throw e;
                    }
                finally {
                        if (redis1 != null) {
                            pool.returnResource(redis1);
                        }
                        if (redis2 != null) {
                            pool.returnResource(redis2);
                        }
                    }
            };
            // process messages
            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

