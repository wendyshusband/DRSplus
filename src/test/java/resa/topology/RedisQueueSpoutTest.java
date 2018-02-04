package resa.topology;

import org.junit.Test;
import redis.clients.jedis.Jedis;
import resa.shedding.tools.TestRedis;

import java.util.Arrays;

import static org.junit.Assert.*;

public class RedisQueueSpoutTest {

    @Test
    public void nextTuple() throws Exception {
        String queue = "fsource";
        Jedis jedis = null;
        try {
            jedis = new Jedis("10.21.25.17", 6379);
        } catch (Exception e) {
        }
        if (jedis == null) {
            System.out.println("FrameSourceFox.Prepare, jedis == null");
            return;
        }
        //jedis.set("time","0");
        int count = 0;
        long c = System.currentTimeMillis();
        while (System.currentTimeMillis() - c <= 1000) {
            count++;
            Object text;
            try {
                text = jedis.lpop(queue);
            } catch (Exception e) {
                return;
            }
            //System.out.println(text);
            if (text != null) {
                String[] tmp = ((String) text).split("[|]");
                double[] v = Arrays.stream(tmp[2].split(",")).mapToDouble((str) -> Double.parseDouble(str)).toArray();
                Integer objId = (int) (Long.parseLong(tmp[0]) % 2);
                Long timestamp = Long.parseLong(tmp[1]);

//                int a;
//                String s = jedis.get("time");
//                a = Integer.valueOf(s);
//                a++;
//                try {
//                    jedis.set("time", String.valueOf(a));
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
            }
        }
        System.out.println(count);
        //System.out.println(System.currentTimeMillis() - c);
    }

}