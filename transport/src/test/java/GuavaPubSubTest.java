import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.sun.corba.se.impl.orbutil.graph.Graph;
import org.junit.Test;
import sun.security.provider.MD5;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by limingxin on 2017/8/1.
 */
public class GuavaPubSubTest {

    @Subscribe
    public void handlerStr(String event) {
        System.out.println("event = [" + event + "]");
    }

    @Subscribe
    public void handlerInt(Integer event) {
        System.out.println("event = [" + event + "]");
    }

    @Test
    public void sync() {
        EventBus bus = new EventBus();
        bus.register(this);
        bus.post("sync hello google");
        bus.post(12);
    }

    @Test
    public void async() {
        AsyncEventBus bus = new AsyncEventBus(Executors.newSingleThreadExecutor());
        bus.register(this);
        bus.post("async hello google");
    }

    @Test
    public void hash() {
        Hasher hasher = Hashing.md5().newHasher();
        hasher.putInt(1);
        hasher.putChar('6');
        hasher.putFloat(2.1F);

        Hasher sha = Hashing.sha512().newHasher();
        sha.putString("hello hash");
        System.out.printf("%s\r\n%s", hasher.hash().toString(), sha.hash().toString());
    }

    @Test
    public void cache() {
        LoadingCache<String, String> cache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(1, TimeUnit.SECONDS)
//                .removalListener(MY_LISTENER)
                .build(
                        new CacheLoader<String, String>() {
                            public String load(String key) throws Exception {
                                return key;
                            }
                        });
        try {
            cache.get("a");
            Thread.sleep(2000L);
            cache.get("a");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
