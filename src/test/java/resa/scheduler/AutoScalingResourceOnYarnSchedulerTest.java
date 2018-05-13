package resa.scheduler;

import org.apache.storm.utils.Utils;
import org.junit.Test;

import java.util.*;

/**
 * Created by 44931 on 2018/2/10.
 */
public class AutoScalingResourceOnYarnSchedulerTest {

    @Test
    public void testIntegerDivided () {
        int numExecutors = -10;
        int numWorkers = 3;
        Map<Integer, Integer> distribution = Utils.integerDivided(numExecutors, numWorkers);
        //System.out.println(distribution);

        double number = Math.ceil(numExecutors * 1.0 / numWorkers);
        //System.out.println(number);

        Map<Integer, Double> t = new HashMap<>();
        t.put(1, 1.0);
        t.put(0,0.0);
        t.put(2,2.0);
        t.put(-2,-2.0);
        t.put(3,3.0);
        //System.out.println(t.values());
        List<Double> nodes = new ArrayList<>();
        for (Map.Entry entry : t.entrySet()) {
            nodes.add((Double) entry.getValue());
        }
        Collections.sort(nodes);
       // System.out.println(nodes);
        Set<Integer> tempExecutorDetails = t.keySet();
        LinkedList<Integer> executors = new LinkedList<>();
        for (Integer tempExecutorDetail: tempExecutorDetails) {
            executors.add(tempExecutorDetail);
        }
        System.out.println(executors);
        executors.pop();
        System.out.println(executors);

    }

}