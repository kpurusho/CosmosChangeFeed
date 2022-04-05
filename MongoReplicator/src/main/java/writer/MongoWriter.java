package writer;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import core.Consumer;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class MongoWriter implements Runnable {
    private final MongoClient mongoClient;
    private final MongoCollection<Document> collection;
    private String shardKey;
    private Consumer receiver;
    AtomicBoolean stop = new AtomicBoolean(false);

    public MongoWriter(String url, String dbName, String collectionName, String shardKey, Consumer receiver) {
        mongoClient = MongoClients.create(url);
        this.shardKey = shardKey;
        MongoDatabase db = mongoClient.getDatabase(dbName);
        collection = db.getCollection(collectionName);
        System.out.println("Current doc count:" + collection.countDocuments());
        this.receiver = receiver;
    }

    public void stop() {
        stop.set(true);
    }

    @Override
    public void run() {
        while (stop.get() == false) {
            List<String> evts = receiver.consume();
            if (evts.stream().count() == 0) continue;
            List<WriteModel<Document>> models = new ArrayList<>();
            for (String evt : evts) {
                Document doc = Document.parse(evt);
                System.out.println(String.format("%s,%s", doc.get("id"), doc.get(shardKey)));
                models.add(getDocument(doc));
            }

            collection.bulkWrite(models);
        }
    }

    private WriteModel<Document> getDocument( Document doc) {
        Map<String, Object> filter = new HashMap<>();
        filter.put("id", doc.get("id"));
        filter.put(shardKey, doc.get(shardKey));
        ReplaceOptions options = new ReplaceOptions();
        options.upsert(true);
        return new ReplaceOneModel<Document>(new Document(filter), doc, options);
    }
}
