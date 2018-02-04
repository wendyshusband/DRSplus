package resa.optimize;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by 44931 on 2018/1/11.
 */
public class ServiceNodeTest {
    @Test
    public void cloneTest() throws Exception {
        BoltAggResult bn1 = new BoltAggResult();
        bn1.setDuration(1);
        bn1.passiveSheddingCountMap.put("p1", 1L);
        bn1.emitCount.put("emit1", 100L);
        bn1.passiveSheddingCountMap.put("p2", 2L);
        bn1.emitCount.put("emit2", 200L);
        ServiceNode serviceNode1 = new ServiceNode("c1",1,1,bn1,0.1);
        ServiceNode serviceNode2 = (ServiceNode) serviceNode1.clone();
        //System.out.println(serviceNode1.getComponentID()+"~"+serviceNode1.passiveSheddingCountMap+"~"+serviceNode1.getEmitCount());
        //System.out.println(serviceNode2.getComponentID()+"~"+serviceNode2.passiveSheddingCountMap+"~"+serviceNode2.getEmitCount());
        serviceNode2.getEmitCount().put("emit2", 300L);
        serviceNode2.getEmitCount().put("emit3", 400L);
        //System.out.println("~"+serviceNode1.getComponentID()+"~"+serviceNode1.passiveSheddingCountMap+"~"+serviceNode1.getEmitCount());
        //System.out.println("~~"+serviceNode2.getComponentID()+"~"+serviceNode2.passiveSheddingCountMap+"~"+serviceNode2.getEmitCount());
        Map<String, ServiceNode> s1 = new HashMap<>();
        s1.put("111",serviceNode1);
        s1.put("222",serviceNode2);
        Map<String, ServiceNode> s2 = cloneServiceNodes(s1);
        //System.out.println("!"+s1);
        //System.out.println("!!"+s2);
        if (s1.equals(s2)) {
          //  System.out.println("eq");
        } else {
           // System.out.println("no eq");
        }
        s2.get("111").getEmitCount().put("emit5", 500L);
        //System.out.println("!!!"+s1);
        //System.out.println("!!!!"+s2);
    }

    static Map<String, ServiceNode> cloneServiceNodes(Map<String, ServiceNode> serviceNodeMap) {
        Map<String, ServiceNode> clone = new HashMap<>();
        for (Map.Entry serviceNode : serviceNodeMap.entrySet()) {
            try {
                clone.put((String) serviceNode.getKey(), (ServiceNode)((ServiceNode) serviceNode.getValue()).clone());
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
        }
        return clone;
    }

}