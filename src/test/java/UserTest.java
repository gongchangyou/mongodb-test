import com.braindata.api.ApiApplication;
import com.braindata.api.model.User;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class UserTest {

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
    public void getAll() {
        val test = db.getCollection("user");
        val all = test.find();
        all.forEach(printBlock);
        log.info("test");
    }

    private ObjectMapper  objectMapper= new ObjectMapper();
    @Test
    public void insert() {
        for (var i = 0L; i < 10L; i++) {
            final long j = i;
            val user = User.builder()
                    .id(i)
                    .name("Eric" + i)
                    .nickname(new ArrayList<>() {{
                        add("a" + j);
                        add("b" + j);
                        add("c" + j);
                    }})
                    .build();
            try {
                db.getCollection("user").insertOne(Document.parse(objectMapper.writeValueAsString(user)));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 批量更新
     * $set 的使用
     */
    @Test
    public void batchUpdate() {
        Bson f = Filters.and(Filters.eq("id", 2),Filters.eq("name", "Eric2"));
        //成功只更新name字段
        db.getCollection("user").updateMany(f, new Document("$set",
                new HashMap<>(){{
                    put("name", "Eric20");
                }}
        ));

        //这里会全量覆盖，不会因为字段是null 就不覆盖。所以updateMany 后就写增量的值吧
//            db.getCollection("user").updateMany(f, new Document("$set",
//                    objectMapper.convertValue(
//                        User.builder().name("Eric10").build()
//                    , Map.class)
//            ));

        getAll();
    }

    /**
     * https://www.mongodb.com/docs/manual/reference/operator/update/addToSet/
     * $addToSet 的使用
     */
    @Test
    public void addToSet() {
        Bson f = Filters.and(Filters.eq("id", 3),Filters.eq("name", "Eric3"));
        db.getCollection("user").updateMany(f, new Document("$addToSet",
                new HashMap<>(){{
                    put("nickname", "d3");
                }}
        ));
        getAll();

        //$each 关键字 在array类型的field中追加 多个element
        db.getCollection("user").updateMany(f, new Document("$addToSet",
                new HashMap<>() {{
                    put("nickname", new HashMap<>(){{
                        put("$each", new ArrayList<>(){{
                            add("e4");
                            add("e5");

                        }});
                    }});
                }}
        ));
        getAll();

    }

    @Test
    public void singleUpdate() {
        Bson f = Filters.and(Filters.eq("id", 4), Filters.eq("name", "Eric4"));
        //替换
        db.getCollection("user").updateMany(f, new Document("$set", new HashMap<>(){{
            put("nickname", new ArrayList() {{
                add("f4");
                add("f5");
            }});
        }} ));
        getAll();

        //.$ 的意思就是数组的下标
        Bson f5 = Filters.and(Filters.eq("id", 5), Filters.eq("nickname", "c5"));
        db.getCollection("user").updateMany(f5, new Document("$set", new HashMap<>(){{
            put("nickname.$", "f5");
        }} ));
        getAll();
    }
}