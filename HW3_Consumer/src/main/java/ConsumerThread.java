import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.nio.charset.StandardCharsets;;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.google.gson.Gson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class ConsumerThread implements Runnable {
    private final static String QUEUE_NAME = "queue2";
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
            // todo: send rate = half of receive
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.queueBind(QUEUE_NAME, "my-fanout-exchange", "");

            // max one message per receiver
            channel.basicQos(1);
            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            Map<String, Map<String, Integer>> skierCounter = new HashMap();
            Map<String, Map<String, Integer>> rideCounter = new HashMap();

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                // keeps a record of the individual lift rides for each skier in a hash map.
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
//                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                System.out.println( "Callback thread ID = " + Thread.currentThread().getId() +
                        " Received '" + message + "'");

                // store resort to redis
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
                        portNumber, 6379, Protocol.DEFAULT_TIMEOUT, "");

                Jedis redis1 = null;
                Jedis redis2 = null;
                try
                {
                    redis1 = pool.getResource();
                    redis2 = pool.getResource();
//                    return redis1.get(keyName);

                    // 1: How many unique skiers visited resort X on day N?
                    if (skierCounter.containsKey(dayID)) {
                        Map<String, Integer> innerMap = skierCounter.get(dayID);
                        if (!innerMap.containsKey(resortID)) {
                            innerMap.replace(resortID, innerMap.get(resortID) + 1);
                        } else {
                            innerMap.put(resortID, 1);
                        }
                    } else {
                        skierCounter.put(dayID, new HashMap<>());
                    }
                    redis1.set(dayID, skierCounter.get(dayID).toString());

                    // 2: How many rides on lift N happened on day N?
                    if (rideCounter.containsKey(dayID)) {
                        Map<String, Integer> innerMap = rideCounter.get(dayID);
                        if (!innerMap.containsKey(liftID)) {
                            innerMap.replace(liftID, innerMap.get(liftID) + 1);
                        } else {
                            innerMap.put(liftID, 1);
                        }
                    } else {
                        rideCounter.put(dayID, new HashMap<>());
                    }
                    redis2.set(dayID, rideCounter.get(dayID).toString());

                    this.map.put(skierID, jsonString);
                }
                catch (JedisConnectionException e) {
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
            //send to exchange, exchange will send to all subscribers
            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
        } catch (IOException e) {
            e.printStackTrace();
        }

//                RedisConnection db = new RedisConnection();
//                Jedis jedis1 = db.getJedis();
//                Jedis jedis2 = db.getJedis();

//                // How many unique skiers visited resort X on day N?
//                if (skierCounter.containsKey(dayID)) {
//                    Map<String, Integer> innerMap = skierCounter.get(dayID);
//                    if (!innerMap.containsKey(resortID)) {
//                        innerMap.replace(resortID, innerMap.get(resortID) + 1);
//                    } else {
//                        innerMap.put(resortID, 1);
//                    }
//                } else {
//                    skierCounter.put(dayID, new HashMap<>());
//                }
//                redis1.set(dayID, skierCounter.get(dayID).toString());

//                // How many rides on lift N happened on day N?
//                if (rideCounter.containsKey(dayID)) {
//                    Map<String, Integer> innerMap = rideCounter.get(dayID);
//                    if (!innerMap.containsKey(liftID)) {
//                        innerMap.replace(liftID, innerMap.get(liftID) + 1);
//                    } else {
//                        innerMap.put(liftID, 1);
//                    }
//                } else {
//                    rideCounter.put(dayID, new HashMap<>());
//                }
//                redis2.set(dayID, rideCounter.get(dayID).toString());
//
//                this.map.put(skierID, jsonString);
//            };
//            // process messages
//            //send to exchange, exchange will send to all subscribers
//            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}