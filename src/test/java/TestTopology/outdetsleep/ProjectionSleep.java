package TestTopology.outdetsleep;

import TestTopology.helper.IntervalSupplier;
import TestTopology.simulated.TASleepBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import resa.util.ConfigUtil;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Created by kailin on 14-3-14.
 */
public class ProjectionSleep extends TASleepBolt {
    public static long start;
    public static final String PROJECTION_ID_FIELD = "projectionId";
    public static final String PROJECTION_VALUE_FIELD = "projectionValue";

    private List<double[]> randomVectors;
    private transient OutputCollector collector;

    private int taskid;
    private int fixMu;
    private String name;
    private int tupleReceiveCount = 0;

    public ProjectionSleep(List<double[]> randomVectors, IntervalSupplier sleep) {
        super(sleep);
        this.randomVectors = randomVectors;
        start = System.currentTimeMillis();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        taskid = context.getThisTaskId();
        this.name = context.getThisComponentId();
        String fixmuconf = "test.fixmu."+name;
        this.fixMu = ConfigUtil.getInt(stormConf, fixmuconf, 1);
        LOG.info("ProjectionSleep is prepared and "+fixmuconf+" is "+fixMu);
    }

    @Override
    public void execute(Tuple input) {
        tupleReceiveCount++;
        if (tupleReceiveCount % fixMu == 0) {
            super.execute(input);
        }
        //System.out.println(a+"projection: "+(a * 1000.0)/ (System.currentTimeMillis() - start));
        Object objId = input.getValueByField(ObjectSpoutSleep.ID_FILED);
        Object time = input.getValueByField(ObjectSpoutSleep.TIME_FILED);
        Object jb = input.getValueByField(ObjectSpoutSleep.JB_FILED);
        double[] v = (double[]) input.getValueByField(ObjectSpoutSleep.VECTOR_FILED);
        //System.out.println(input.getSourceComponent()+"projectionuuuid:"+jb);
        IntStream.range(0, randomVectors.size()).forEach((i) -> {
            collector.emit(input, new Values(jb, objId, i, innerProduct(randomVectors.get(i), v), time));
        });

        collector.ack(input);
    }

    private static double innerProduct(double[] v1, double[] v2) {
        if (v1.length != v2.length) {
            throw new IllegalArgumentException();
        }
        return IntStream.range(0, v1.length).mapToDouble((i) -> v1[i] * v2[i]).sum();
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ObjectSpoutSleep.JB_FILED, ObjectSpoutSleep.ID_FILED, PROJECTION_ID_FIELD,
                PROJECTION_VALUE_FIELD, ObjectSpoutSleep.TIME_FILED));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
