import com.azure.cosmos.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Date;
import java.util.Random;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

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

    public static final String dbName = "flight";
    public static final String containerName = "schedule";

    public static final String checkpointDbName = "checkpoint";
    public static final String cpContainerName = "session";

    public static final String mongoUrl = "mongodb://kartmongo:YQXIxujXU7H6zgnT26Yq5vY6eHNUDcFZZbBJd7WJIDokOhj3DpipoEDBdkSJ4SoVBQP1ibySUN27m5dKrWSnPA==@kartmongo.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@kartmongo@";
    public static final String mongoDbName = "flight";
    public static final String mongoCollectionName = "status";
    public static final String shardkey = "finalcity";

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
        CosmosContainer container = cosmosClient.getDatabase(dbName).getContainer(containerName);

        int startId = 1;
        for (String a : args) {
            startId = Integer.valueOf(a);
            break;
        }
        if (startId == 1) {
            prepareSources(cosmosClient, 11);
            System.exit(0);
        }
        System.out.println("Press any key to insert, q to quit");
        while (true) {
            int c = System.in.read();
            if (c == 'q') break;
            Schedule s = createSchedule(startId++);
            upsert(cosmosClient, container, s);
        }

        System.out.println("Done updating..");
        System.exit(0);
    }

    private static void upsert(CosmosClient cosmosClient, CosmosContainer container, Schedule s) {
        System.out.println(String.format("Inserting: %s,%s", s.id, s.finalcity));
        try {
            container.upsertItem(s);
        }
        catch (Exception ex) {
            container = cosmosClient.getDatabase(dbName).getContainer(containerName);
            container.upsertItem(s);
        }
    }

    private static void prepareSources(CosmosClient cosmosClient, int startId) {
        recreateSource(cosmosClient, startId);
        recreateCPContainer(cosmosClient);
        recreateMongoCollection();
    }

    private static void recreateMongoCollection() {
        try {
            MongoClient mongoClient = MongoClients.create(mongoUrl);
            MongoDatabase mongoDb = mongoClient.getDatabase(mongoDbName);
            MongoCollection collection = mongoDb.getCollection(mongoCollectionName);
            collection.deleteMany(new Document());
        }
        catch (Exception ignored) {}
    }

    private static void recreateCPContainer(CosmosClient cosmosClient) {
        CosmosDatabase dbCp = cosmosClient.getDatabase(checkpointDbName);
        CosmosContainer cpContainer = dbCp.getContainer(cpContainerName);
        try {
            cpContainer.delete();
        }
        catch (Exception ignored) {}
        dbCp.createContainerIfNotExists(cpContainerName, "/id");
    }

    private static void recreateSource(CosmosClient cosmosClient, int startId) {
        CosmosDatabase db = cosmosClient.getDatabase(dbName);
        CosmosContainer container = db.getContainer(containerName);
        try {
            container.delete();
        }
        catch (Exception ignored) {}
        db.createContainerIfNotExists(containerName, "/finalcity");
        populateSource(container);

        for (int i = 1; i < startId; i++) {
            Schedule s = createSchedule(i);
            upsert(cosmosClient, container, s);
        }
    }

    private static void populateSource(CosmosContainer container) {

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
