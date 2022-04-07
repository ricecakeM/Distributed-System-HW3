import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.util.concurrent.ConcurrentHashMap;

public class Consumer {
    private static final int THREAD_COUNT = 10; //TODO:make quene length closer to 0

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
//        factory.setHost("localhost");
//        factory.setPort(5672);
//        factory.setUsername("guest");
//        factory.setPassword("guest");
//        factory.setVirtualHost("/");
        factory.setHost("35.163.110.224"); //todo: rabbitMQ ip
        factory.setUsername("admin");
        factory.setPassword("password");
        Connection connection = factory.newConnection();
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

        for (int i = 0; i < THREAD_COUNT; i++) {
            new Thread(new ConsumerThread(connection, map)).start();
        }
    }
}
//test consumer, client, look at rabbitmq console,
//todo: test local consumer (listen),

