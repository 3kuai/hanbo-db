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
 * 性能比拼接近于等价，争取超过redis！fighting！！
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = MainApplication.class)
@Slf4j
public class RedisTests {

    @Autowired
    RedisTemplate<String, String> template;
    ExecutorService es = Executors.newFixedThreadPool(8);
    AtomicInteger count = new AtomicInteger(0);

    @Test
    public void test() throws Exception {
        for (int i = 1000; i < 2000; ++i) {
            es.execute(new Runnable() {
                @Override
                public void run() {
                    int i = count.incrementAndGet();
//                    template.opsForValue().set("aa" + i, "b" + i);
                    log.info("k=aa" + i + ",v=" + template.opsForValue().get("aa" + i));
//                    template.opsForList().leftPushAll("list" + i, "1", "2", "3");
                    log.info("list=" + template.opsForList().range("list", 0, -1));
//                    template.opsForHash().put("user200" + i, "age", "25");
//                    template.opsForHash().put("user200" + i, "sex", "男");
                    log.info("hv=" + template.opsForHash().get("user200" + i, "age"));
                }
            });
        }
        es.awaitTermination(5, TimeUnit.SECONDS);
    }

}
