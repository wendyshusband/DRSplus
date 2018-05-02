package TestTopology.outdetsleep;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import resa.shedding.tools.TestRedis;
import resa.topology.RedisQueueSpout;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * Created by kailin on 14-3-14.
 */
public class ObjectSpoutSleep extends RedisQueueSpout {
    public static final String JB_FILED = "jb";
    public static final String ID_FILED = "id";
    public static final String VECTOR_FILED = "vector";
    public static final String TIME_FILED = "time";
    private final int objectCount;
    private static Jedis jedis = TestRedis.getJedis();
    //private static AtomicInteger jb;


    public ObjectSpoutSleep(String host, int port, String queue, int objectCount) {
        super(host, port, queue);
        this.objectCount = objectCount;
        System.out.println("objcOunt "+objectCount);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        //testGetIDThread();
        //jb = new AtomicInteger(Integer.valueOf(jedis.get("time")));
    }

    @Override
    protected void emitData(Object data) {
        String[] tmp = ((String) data).split("[|]");
        double[] v = Arrays.stream(tmp[2].split(",")).mapToDouble((str) -> Double.parseDouble(str)).toArray();
        Integer objId = (int) (Long.parseLong(tmp[0]) % objectCount);
        Long timestamp = Long.parseLong(tmp[1]);
        //jb.getAndIncrement();
        int jb;
        String s = jedis.get("time");
        jb = Integer.valueOf(s);
        jb++;
        try {
            TestRedis.add("time", String.valueOf(jb));
        } catch (Exception e) {
            e.printStackTrace();
        }
        collector.emit(new Values(jb, objId, v, timestamp), Collections.singletonMap(objId, timestamp));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(JB_FILED, ID_FILED, VECTOR_FILED, TIME_FILED));
    }

//    private void testGetIDThread() {
//        final Thread thread = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                boolean done = false;
//                int ID , a;
//                String s;
//                while (!done){
//                    ID = jb.get();
//                    s = jedis.get("time");
//                    a = Integer.valueOf(s);
//                    System.out.println(ID+"shibushiyaoda???"+a);
//                    if (ID > a) {
//                        try {
//                            TestRedis.add("time", String.valueOf(ID));
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                    }
//                }
//            }
//        });
//        thread.setDaemon(true);
//        thread.start();
//        System.out.println("testGetIDThread thread start!");
//    }
}

//