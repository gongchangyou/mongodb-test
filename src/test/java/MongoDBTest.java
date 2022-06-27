import com.braindata.api.ApiApplication;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.util.Arrays;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author gongchangyou
 * @version 1.0
 * @date 2022/3/15 9:41 上午
 */

@Slf4j
@SpringBootTest(classes= ApiApplication.class)
public class MongoDBTest {

    //mongodb 客户端
    private MongoDatabase db;

    @PostConstruct
    public void init() {
        MongoCredential credential = MongoCredential.createCredential("admin", "admin", "123456".toCharArray());
        MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 27017),
                credential, new MongoClientOptions.Builder().build());
        db = mongoClient.getDatabase("admin");//这个对应的是db
    }

    private Consumer<Document> printBlock = new Consumer<Document>() {
        @Override
        public void accept(final Document document) {
            log.info("document {}", document);
        }
    };

    @Test
    public void test() {
        val test = db.getCollection("records");
        val all = test.find();
        all.forEach(printBlock);
        log.info("test");
    }

    @Test
    public void insert() {
        Random random = new Random();
        //写入数据
        for (int i=20000;i<100000;i++) {
            val list = new ArrayList<Document>();
            for (long k= i *1000; k< (i+1)*1000;k++) {
                val p = k;
                var x = random.nextDouble();//0-1
                var y = random.nextDouble();//0-1
                if (x<0.5) { //往中心偏移
                    x += Math.abs(0.5f -x) / 2;
                }else {
                    x -= Math.abs(0.5f -x) / 2;
                }

                if (y<0.5) { //往中心偏移
                    y += Math.abs(0.5f -y) / 2;
                }else {
                    y -= Math.abs(0.5f -y) / 2;
                }
                val count = random.nextInt(5);//0-5
                val xx = x* 10000;
                val yy = y * 10000;
                list.add(new Document(new HashMap<>() {{
                    put("id", String.valueOf(p));
                    log.info("id={}", p);
                    put("loc", new ArrayList<Double>() {{
                        add(xx);
                        add(yy);
                    }});
                    put("count", count);
                }}));
            }
            db.getCollection("records").insertMany(list);
            log.info("i={}", i);
        }
    }

    public static <T> List<List<T>> split(List<T> list, int size) {
        final AtomicInteger counter = new AtomicInteger();
        return new ArrayList<>(
                list.stream()
                        .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / size))
                        .values());
    }

    @Test
    public void batchGet() {
        val table =db.getCollection("records");
        val ids = new ArrayList<>();
        for (int i = 20140000; i <20147282; i++) {
            ids.add(String.valueOf(i));
        }
        val chunkedList = split(ids, 100);
        val firstStart = System.currentTimeMillis();
        for (val list : chunkedList) {
            val start = System.currentTimeMillis();
            BasicDBObject query = new BasicDBObject("id", new BasicDBObject("$in",list) );
            table.find(query).forEach((Consumer<Document>) document ->{
                log.info("id={}, loc={}",document.get("id"),  document.get("loc"));
            });
            log.info("cost={}", System.currentTimeMillis()- start);
        }
        log.info("allcost={}", System.currentTimeMillis()- firstStart);
    }

    @Test
    public void singleGet() {
        val firststart = System.currentTimeMillis();
        val table =db.getCollection("records");
        for (int i = 20140000; i <20147282; i++) {
            val start = System.currentTimeMillis();
            BasicDBObject query = new BasicDBObject("id",  String.valueOf(i));
            log.info("i={}", i);
            table.find(query).forEach((Consumer<Document>) document ->{
                log.info("loc={}", document.get("loc"));
            });
            log.info("cost={}", System.currentTimeMillis()- start);
        }
        log.info("allcost={}", System.currentTimeMillis()- firststart);
    }

    private ThreadPoolExecutor executor = new ThreadPoolExecutor(
            10
            , 10
            , 0L
            , TimeUnit.MILLISECONDS
            , new LinkedBlockingQueue(250)
            , new ThreadPoolExecutor.CallerRunsPolicy()
    );

    /**
     * 搜索
     * db.records.find( {
     *    loc: { $geoWithin: { $box:  [ [ 0, 0 ], [ 50, 100 ] ] } }
     * } )
     */
    @Test
    public void search() {
        for (int i = 400; i <1000;i++) {
            for (int j=i;j<1000; j++) {
                val ii = i;
                val jj = j;
                executor.submit(() -> {
                    val start = System.currentTimeMillis();
                    val box = new ArrayList();
                    box.add(new double[]{ii * 10, jj * 10}); //Starting coordinate
                    box.add(new double[]{(ii + 1) * 10, (jj + 1) * 10}); // Ending coordinate
                    BasicDBObject query = new BasicDBObject("loc", new BasicDBObject("$geoWithin",
                            new BasicDBObject("$box", box)));
//                    log.info("box={}", box.stream().findFirst());
                    var list = new ArrayList<Integer>();
                    db.getCollection("records").find(query).forEach(
                            (Consumer<Document>) document -> {
                                list.add(Integer.valueOf(document.get("count").toString()));
                                log.info("ii={}, jj={}, count={}", ii, jj, document.get("count"));
                            }
                    );
                    val end = System.currentTimeMillis();
                    log.info("ii={}, jj={}, size={}, count={}, cost={}",
                            ii, jj,list.size(), list.stream().mapToInt(p->p).sum(), end-start);

                });
            }
        }

    }
}