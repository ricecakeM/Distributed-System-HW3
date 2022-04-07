import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class SkierThread implements Runnable {
    private int startID;
    private int endID;
    private int startTime;
    private int endTime;
    private int numRuns;
    private int numSkiers;
    private int numThreads;
    private int numLifts;
    private int resortID;
    private String seasonID;
    private String dayID;
    private int numPostRequests;
    private int numWaitedThreads;
    private CountDownLatch privateLatch;
    private CountDownLatch sharedLatch;
    private List<Double> responseTimes;
    private AtomicInteger totalSuccessfulPosts;
    private AtomicInteger totalFailedPosts;
//    private final String BASE_URL = "http://18.237.32.127:8080/lab2_war";
//    private final String BASE_URL = "http://52.40.6.4:8080/lab2_war";
    private final String BASE_URL = "http://homework2-195325968.us-west-2.elb.amazonaws.com:8080/lab2_war";

    public SkierThread(int startID, int endID, int startTime, int endTime, int numRuns,
                       int numSkiers, int numThreads, int numLifts, int resortID, String seasonID,
                       String dayID, int numPostRequests, int numWaitedThreads, CountDownLatch privateLatch,
                       CountDownLatch sharedLatch, List<Double> responseTimes, AtomicInteger totalSuccessfulPosts,
                       AtomicInteger totalFailedPosts) {
        this.startID = startID;
        this.endID = endID;
        this.startTime = startTime;
        this.endTime = endTime;
        this.numRuns = numRuns;
        this.numSkiers = numSkiers;
        this.numThreads = numThreads;
        this.numLifts = numLifts;
        this.resortID = resortID;
        this.seasonID = seasonID;
        this.dayID = dayID;
        this.numPostRequests = numPostRequests;
        this.numWaitedThreads = numWaitedThreads;
        this.privateLatch = privateLatch;
        this.sharedLatch = sharedLatch;
        this.responseTimes = responseTimes;
        this.totalSuccessfulPosts = totalSuccessfulPosts;
        this.totalFailedPosts = totalFailedPosts;
    }

    @Override
    public void run() {
        Timestamp startTime, endTime;

        SkiersApi apiInstance = new SkiersApi();
        ApiClient client = apiInstance.getApiClient();
        client.setBasePath(BASE_URL);

        int localTotalSuccess = 0;
        int localFailedPosts = 0;
        for (int i = 0; i < this.numPostRequests; i++) {
            LiftRide liftRide = new LiftRide();
            liftRide.setTime(ThreadLocalRandom.current().nextInt(this.endTime - this.startTime) + this.startTime);
            liftRide.setLiftID(ThreadLocalRandom.current().nextInt(this.numLifts) + 1);
            int skierID = ThreadLocalRandom.current().nextInt(this.endID - this.startID) + this.startID;
            int statusCode;
            startTime = new Timestamp(System.currentTimeMillis());
            try {
                int count = 0;
                // logic for retry
                while (true) {
                    count += 1;
                    // send post request
                    ApiResponse response = apiInstance.writeNewLiftRideWithHttpInfo(liftRide, this.resortID,
                            this.seasonID, this.dayID, skierID);
                    statusCode = response.getStatusCode();

                    if (statusCode == 201) {
                        localTotalSuccess += 1;
                        break;
                    } else if (statusCode >= 400 && statusCode < 600) {
                        continue;
                    }
                    if (count == 6) {
                        localFailedPosts += 1;
                        break;
                    }
                }
                endTime = new Timestamp(System.currentTimeMillis());
            } catch (ApiException e) {
                System.err.println("Exception when calling SkierApi writeNewLiftRide");
                endTime = new Timestamp(System.currentTimeMillis());
                localFailedPosts += 1;
                e.printStackTrace();
            }
            double latency = endTime.getTime() - startTime.getTime();
            this.responseTimes.add(latency);
        }
        this.totalSuccessfulPosts.addAndGet(localTotalSuccess);
        this.totalFailedPosts.addAndGet(localFailedPosts);
        try {
            this.privateLatch.countDown();
            this.sharedLatch.countDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
