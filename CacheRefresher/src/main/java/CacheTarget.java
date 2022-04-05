import core.CacheAccessor;
import core.EventHubConsumer;
import core.RedisCacheAccessor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import refresher.*;

public class CacheTarget {

    public static final String cacheHost = "kartredis.redis.cache.windows.net";
    public static final String cacheKey = "9IcErru3o72k5ICLptctL59DZauBNiDtwAzCaPgtf6Y=";

    public static final String eventEndPoint = "Endpoint=sb://kartevent.servicebus.windows.net/;SharedAccessKeyName=eventpolicy;SharedAccessKey=xtyonwvbR7SItuFlbHzkk0xkcvvblwSV3UwpTgXYfP8=;EntityPath=employeehub";
    public static final String eventHub = "employeehub";
    public static final int eventHubPartitionCount = 2;

    public static void main(String[] args) throws IOException, InterruptedException {
        List<EventHubConsumer> receivers = getReceivers(eventHubPartitionCount);
        List<Thread> writerThreads = new ArrayList<>();
        List<CacheRefresher> writers = new ArrayList<>();
        List<RedisCacheAccessor> accessors = new ArrayList<>();
        RedisCacheAccessor accessor = new RedisCacheAccessor(cacheHost, cacheKey);
        accessors.add(accessor);
        for (EventHubConsumer receiver : receivers) {
            RedisCacheAccessor a = new RedisCacheAccessor(cacheHost, cacheKey);
            CacheRefresher refresher = new CacheRefresher(a, receiver);
            accessors.add(a);
            writers.add(refresher);
            Thread t = new Thread(refresher);
            t.start();
            writerThreads.add(t);
        }
        System.out.println("Enter id to fetch or q to exit");
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(System.in));
        while (true) {
            String ip = reader.readLine();
            if (ip.equals("q")) break;
            try {
                Integer.parseInt(ip);
                System.out.println(accessor.get(ip));
            } catch (NumberFormatException e) {
                System.out.println("Invalid input, Enter id to fetch or q to exit");
            }
        }
        for (refresher.CacheRefresher writer : writers) {
            writer.stop();
        }
        for (EventHubConsumer receiver : receivers) {
            receiver.close();
        }
        for (RedisCacheAccessor a : accessors) {
            a.close();
        }
        for (Thread t : writerThreads) {
            t.join();
        }
        System.out.println("Done receiving..");
        System.exit(0);
    }

    private static List<EventHubConsumer> getReceivers(int partitionCount) {
        List<EventHubConsumer> receivers = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            receivers.add(new EventHubConsumer(eventEndPoint, String.format("%d", i), "cacheconsumer"));
        }
        return receivers;
    }
}
