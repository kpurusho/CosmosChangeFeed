package refresher;

import core.CacheAccessor;
import core.Consumer;
import org.json.JSONObject;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class CacheRefresher  implements Runnable {
    private CacheAccessor cacheAccessor;
    private Consumer receiver;
    AtomicBoolean stop = new AtomicBoolean(false);

    public CacheRefresher(CacheAccessor cacheAccessor, Consumer receiver) {
        this.cacheAccessor = cacheAccessor;
        this.receiver = receiver;
    }
    @Override
    public void run() {
        while (stop.get() == false) {
            List<String> evts = receiver.consume();
            if (evts.stream().count() == 0) continue;
            for (String evt : evts) {
                JSONObject obj = new JSONObject(evt);
                String id = obj.get("id").toString();
                cacheAccessor.set(id, evt);
                System.out.println(String.format("%s,%s", obj.get("id"), obj.get("finalcity")));
            }
        }
    }

    public void stop() {
        stop.set(true);
    }

}
