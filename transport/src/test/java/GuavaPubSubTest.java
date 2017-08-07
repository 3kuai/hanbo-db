import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.junit.Test;
import sun.security.provider.MD5;

import java.util.concurrent.Executors;

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
}
