import com.google.gson.Gson;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

@WebServlet(name = "SkierServlet", value = "/SkierServlet")
public class SkierServlet extends HttpServlet {
    private final static String QUEUE_NAME = "myQueue";
    private ObjectPool<Channel> pool;
    private static Connection connection;


    //init method creates connections and channel pool
    @Override
    public void init() throws ServletException {
        super.init();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("54.214.119.191"); //todo:rabbitmq ipv4
        // baseurl dns http://homework2-195325968.us-west-2.elb.amazonaws.com:8080/
        factory.setUsername("admin"); //
        factory.setPassword("password"); //
        factory.setVirtualHost("/");
        try {
            connection = factory.newConnection();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
        GenericObjectPoolConfig<Channel> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(10);
        config.setMaxIdle(10);
        pool = new GenericObjectPool<>(new ChannelFactory(), config);
    }

    private static class ChannelFactory extends BasePooledObjectFactory<Channel> {

        @Override
        public Channel create() throws IOException {
            return connection.createChannel();
        }

        @Override
        public PooledObject<Channel> wrap(Channel channel) {
            return new DefaultPooledObject<>(channel);
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        PrintWriter out = response.getWriter();
        String urlPath = request.getPathInfo();
        Gson gson = new Gson();

        // check we have a URL
        if (urlPath == null || urlPath.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            Message responseMsg = new Message("URL not found");
            out.write(gson.toJson(responseMsg));
            return;
        }

        String[] urlParts = urlPath.split("/");
        // and now validate url path and return the response status code
        if (!isUrlValid(urlParts)) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            Message responseMsg = new Message("Invalid URL");
            out.write(gson.toJson(responseMsg));
        } else {
            response.setStatus(HttpServletResponse.SC_CREATED); // 201
            int resortId = Integer.parseInt(urlParts[1]);
            int seasonId = Integer.parseInt(urlParts[3]);
            int dayId = Integer.parseInt(urlParts[5]);
            int skierId = Integer.parseInt(urlParts[7]);
            // send to queue
            Channel channel = null;
            try {
                // borrow from pool
                channel = pool.borrowObject();
                BufferedReader reader = request.getReader();
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                    StringBuilder sb = new StringBuilder();
                    sb.append(line);
                    String message = resortId + "!" + seasonId + "!" + dayId + "!" + skierId + "!" + sb;

                    // exchange fanout
                    channel.exchangeDeclare("my-fanout-exchange", "fanout");
                    channel.basicPublish("my-fanout-exchange", "", null, message.getBytes());
                    System.out.println(" [x] Sent '" + message + "'");
                }
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Unable to borrow buffer from pool" + e);
            } finally {
                // return to pool
                if (channel != null) {
                    try {
                        pool.returnObject(channel);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            out.flush();
        }
    }

        private boolean isUrlValid (String[]urlParts){
            // validate the post request url path according to the API spec
            // urlPath= "/skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}"
            // urlParts = [, resorts, 1, seasons, 1, days, 1, skiers, 1]
            if (urlParts.length == 8) {
                for (int i = 1; i < urlParts.length; i += 2) {
                    try {
                        Integer.parseInt(urlParts[i]);
                    } catch (NumberFormatException e) {
                        return false;
                    }
                }
                return urlParts[2].equals("seasons")
                        && urlParts[4].equals("days")
                        && urlParts[6].equals("skiers")
                        && Integer.parseInt(urlParts[7]) <= 100000;
            }
            return false;
        }

        @Override
        protected void doGet (HttpServletRequest req, HttpServletResponse res)
            throws ServletException, IOException {
            res.setContentType("text/plain");
            String urlPath = req.getPathInfo();
            res.getWriter().write(urlPath);

            // check we have a URL!
            if (urlPath == null || urlPath.isEmpty()) {
                res.setStatus(HttpServletResponse.SC_NOT_FOUND);
                res.getWriter().write("missing parameters");
                return;
            }
            String[] urlParts = urlPath.split("/");
            // and now validate url path and return the response status code
            if (!isUrlValid(urlParts)) {
                res.setStatus(HttpServletResponse.SC_NOT_FOUND);
                res.getWriter().write("It does not work! " + Arrays.toString(urlParts));
            } else {
                res.setStatus(HttpServletResponse.SC_OK);
                res.getWriter().write("It works!");
            }
        }
    }
