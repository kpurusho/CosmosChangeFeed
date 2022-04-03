import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.ThrottlingRetryOptions;

import java.io.IOException;
import java.time.Duration;
import java.util.Date;
import java.util.Random;

class Schedule {
    public String id;
    public String startcity;
    public String finalcity;
    public String flightcode;
    public Date departure;
    public Date arrival;
    public String status;
}

public class FlightUtil {
    public static final String cosmosEndPoint = "https://kartcosmossql.documents.azure.com:443/";
    public static final String cosmosKey = "XFmJKi0cquhDZ47QQHXmdRuWrIR9RzPqXGwRFvhJmHaEwmSrXaFW4BSHA48OLCfJ90hHeoExBNPQaOY6Q5dBag==";

    public static String[] cities = {"Agra", "Delhi", "Madurai", "Coimbatore"};
    public static String[] dest = {"Chennai", "Mumbai"};

    public static void main(String[] args) throws IOException, InterruptedException {
        ThrottlingRetryOptions retryOptions = new ThrottlingRetryOptions();
        retryOptions.setMaxRetryWaitTime(Duration.ofSeconds(60));
        retryOptions.setMaxRetryAttemptsOnThrottledRequests(20);
        CosmosClient cosmosClient = new CosmosClientBuilder()
                .endpoint(cosmosEndPoint)
                .key(cosmosKey)
                .throttlingRetryOptions(retryOptions)
                .buildClient();
        CosmosContainer container = cosmosClient.getDatabase("flight").getContainer("schedule");
        int startId = 1;
        if (args.length > 1) {
            startId = Integer.valueOf(args[0]);
        }
        System.out.println("Press any key to insert, q to quit");
        while (true) {
            int c = System.in.read();
            if (c == 'q') break;
            Schedule s = createSchedule(startId++);
            System.out.println(String.format("Inserting: %s,%s", s.id, s.finalcity));
            try {
                container.upsertItem(s);
            }
            catch (Exception ex) {
                container.upsertItem(s);
            }
        }

        System.out.println("Done updating..");
        System.exit(0);
    }

    private static Schedule createSchedule(int i) {
        Random r = new Random();
        Schedule s = new Schedule();
        s.startcity = cities[r.nextInt(3)];
        s.finalcity = dest[i%2];
        s.id = String.format("%d",i);
        s.flightcode = String.format("F%d", i);
        s.departure = new Date();
        s.arrival = new Date();
        s.status = "Scheduled";
        return s;
    }
}
