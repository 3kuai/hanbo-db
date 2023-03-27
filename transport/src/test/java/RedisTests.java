import com.lmx.jredis.storage.DataTypeEnum;
import com.lmx.jredis.transport.MainApplication;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * java系统 固态硬盘<br>
 * &nbsp;日志级别error
 * &nbsp;处理1000*7次读写操作（覆盖kv list hash） 1900ms 1949ms
 * &nbsp;读2000*3次耗时1945ms<br>
 * <p>
 * java系统 机械硬盘<br>
 * &nbsp;日志级别error
 * &nbsp;处理1000*7次读写操作（覆盖kv list hash） 2280ms 2319ms
 * &nbsp;读2000*3次耗时1945ms<br>
 * <p>
 * redis系统 固态硬盘<br>
 * &nbsp;处理1000*7次读写操作（覆盖kv list hash） 耗时1266ms 1675ms
 * &nbsp;读2000*3次耗时2200ms<br>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = MainApplication.class)
@Slf4j
public class RedisTests {

    @Autowired
    RedisTemplate<String, String> template;
    ExecutorService es = Executors.newFixedThreadPool(64);
    AtomicInteger count = new AtomicInteger(0);

    @Test
    public void test() throws Exception {
        for (int i = 0; i < 100 * 1000; ++i) {
            es.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        int i = count.incrementAndGet();
                        template.opsForValue().set("aa" + i, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb11123aaa" + i);
                        template.opsForValue().get("aa" + i);
//                        log.error("k=aa" + i + ",v=" + template.opsForValue().get("aa" + i));
//                        template.opsForList().leftPush(DataTypeEnum.LIST.getDesc() + i, "test");
//                        log.error("list=" + template.opsForList().range(DataTypeEnum.LIST.getDesc() + i, 0, -1));
                        template.opsForHash().put("user200" + i, "age", "25");
                        template.opsForHash().put("user200" + i, "sex", "男asdsadas123123fff");
//                        log.error("hv=" + template.opsForHash().get("user200" + i, "age"));
                        template.opsForHash().get("user200" + i, "age");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        es.shutdown();
        es.awaitTermination(60, TimeUnit.SECONDS);
    }

}
