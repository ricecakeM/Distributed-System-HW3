import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.util.concurrent.ConcurrentHashMap;

public class Consumer {
    private static final int THREAD_COUNT = 100; //TODO:make queue length closer to 0

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
//        factory.setHost("localhost");
//        factory.setPort(5672);
//        factory.setUsername("guest");
//        factory.setPassword("guest");
//        factory.setVirtualHost("/");
        factory.setHost("54.214.119.191"); // todo:rabbit mq ipv4
        factory.setUsername("admin");
        factory.setPassword("password");
        factory.setVirtualHost("/");
        Connection connection = factory.newConnection();
        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

        for (int i = 0; i < THREAD_COUNT; i++) {
            new Thread(new ConsumerThread(connection, map)).start();
        }
    }
}


