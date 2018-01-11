package resa.shedding.basicServices;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import resa.optimize.AllocResult;
import resa.optimize.ServiceNode;
import resa.optimize.SourceNode;
import resa.shedding.basicServices.api.AbstractTotalCost;
import resa.shedding.basicServices.api.AllocationAndActiveShedRatios;
import resa.shedding.basicServices.api.ICostFunction;
import resa.shedding.basicServices.api.LearningModel;
import resa.shedding.tools.HistoricalAdjustRatioMMK;
import resa.shedding.tools.TestRedis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.DoublePredicate;
import java.util.stream.Collectors;

/**
 * Created by kailin on 12//17
 */
public class SheddingMMKServiceModel implements SheddingServiceModel {

    private static final Logger LOG = LoggerFactory.getLogger(SheddingMMKServiceModel.class);
    private static final int HISTORY_SIZE = 100;
    private static final int ADJUST_RATIO_FUNCTION_ORDER = 1;
    private static final int LEARNLING_THRESHOLD = 5;
    private static final int DEVIATION_RATIO = 2;
    private static final double SHED_RATIO_UNIT = 0.01;
    private HistoricalAdjustRatioMMK paramPairForCalcAdjRatio = new HistoricalAdjustRatioMMK(HISTORY_SIZE);
    private static Jedis jedis = TestRedis.getJedis();
    private static boolean testflag = true;
    /**
     * We assume the stability check for each node is done beforehand!
     * Jackson OQN assumes all the arrival and departure are iid and exponential
     *
     * Note, the return time unit is in Second!
     *
     * @param serviceNodes, the service node configuration, in this function, chain topology is assumed.
     * @param allocation,   the target allocation to be analyzed
     * @return here we should assume all the components are stable, the stability check shall be done outside this function
     */
    static double getExpectedTotalSojournTimeForJacksonOQN(Map<String, ServiceNode> serviceNodes, Map<String, Integer> allocation) {
        //System.out.println(serviceNodes+"wanlilaiwangzhederongyao: "+allocation);
        double retVal = 0.0;
        for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
            String cid = e.getKey();
            ServiceNode serviceNode = e.getValue();
            int serverCount = allocation.get(cid).intValue();
            double avgSojournTime = sojournTime_MMK(serviceNode.getLambda(), serviceNode.getMu(), serverCount);
            retVal += (avgSojournTime * serviceNode.getRatio());
        }
        return retVal;
    }

    static double fitEstimateRatio(double estimateTSecs, double[] cofees) {//estimateTSecs
        double adjEstimateTMilliSec = 0.0;
        for (int j=0; j<cofees.length; j++) {
            //System.out.println(Math.pow(estimateTSecs * 1000.0, j)+"///"+cofees[j]);
            adjEstimateTMilliSec += Math.pow(estimateTSecs * 1000.0, j) * cofees[j];
        }
        return adjEstimateTMilliSec;
    }

    static Map initActiveRateMap(SourceNode sourceNode, Map<String, ServiceNode> queueingNetwork) {
        Map<String, Double> activeShedRateMap = new HashMap<>();
        activeShedRateMap.put(sourceNode.getComponentID(),0.0);
        queueingNetwork.keySet().stream().forEach(e -> {
            activeShedRateMap.put(e, 0.0);
        });
        return activeShedRateMap;
    }
//    static double binarySearchShedRate (SourceNode sourceNode, Map<String, ServiceNode> serviceNodes, double tolerant,
//                                        double lambda, double highMark, double lowMark, double[] adjRatioArr,
//                                        int totalResourceCount, double completeTimeMilliSecUpper) {
//
//        return 0.0;
//    }
//    static AllocationAndActiveShedRatios calcResult(boolean flag, SourceNode sourceNode, Map<String, ServiceNode> serviceNodes, int totalResourceCount,
//                                                             double completeTimeMilliSecUpper, double tolerant, double[] adjRatioArr) {
//        Map<String, Integer> tempAllocation = new HashMap<>();
//        double originLambda0 = sourceNode.getTupleEmitRateOnSQ();
//        Map<String, Double> activeShedRateMap = initActiveRateMap(sourceNode, serviceNodes);
//        for (String comp : activeShedRateMap.keySet()) {
////            double estimateTSecs;
////            double lowMark = 0.0;
////            double lambda;
//            if (comp.equals(sourceNode.getComponentID())) {
//                binarySearchMinimizedShedRate();
//            } else {
//
//                double highMark = serviceNodes.get(comp).getLambda();
//                lambda = (lowMark + highMark) / 2.0;
//                double load;
//                while (lambda >= 1 && lambda > lowMark && highMark > lambda) {
//                    load = lambda / serviceNodes.get(comp).getRatio();
//                    for (ServiceNode serviceNode : serviceNodes.values()) {
//                        serviceNode.changeLambdaAndOtherRelateParam(load * serviceNode.getRatio(), load);
//                    }
//
//                    tempAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes, totalResourceCount);
//
//                    if (tempAllocation != null) {
//                        estimateTSecs = getExpectedTotalSojournTimeForJacksonOQN(serviceNodes, tempAllocation);
//                        double adjEstimateTMilliSec = 0.0;
//                        for (int j = 0; j < adjRatioArr.length; j++) {
//                            adjEstimateTMilliSec += Math.pow(estimateTSecs * 1000.0, j) * adjRatioArr[j];
//                        }
//                        System.out.println(estimateTSecs + " old adjEstimateTMilliSec = " + adjEstimateTMilliSec);
//                        if (Math.abs(adjEstimateTMilliSec - completeTimeMilliSecUpper) <= (1.0 - tolerant) * completeTimeMilliSecUpper) {
//                            break;
//                        } else {
//                            if (adjEstimateTMilliSec > completeTimeMilliSecUpper || adjEstimateTMilliSec < 0) {
//                                highMark = lambda;
//
//                            } else {
//                                lowMark = lambda;
//                            }
//                        }
//                    } else {
//                        highMark = lambda;
//                    }
//                    lambda = (lowMark + highMark) / 2.0;
//                }
//            }
//        }
//        return null;
//    }

    /**
     * when we shedding on a bolt, its downstream bolt must update simultaneous.
     * */
    private static void updateServiceNode(SourceNode sourceNode, Map<String, ServiceNode> serviceNodes, double lambda, ServiceNode selectNode, Map<String, RevertRealLoadData> revertRealLoadDatas, Map<String, double[]> selectivityFunctions) {
        //System.out.println("wangyihuyuselect:   "+selectNode.getComponentID());
        for (Map.Entry r : revertRealLoadDatas.entrySet()) {
            if (((RevertRealLoadData)r.getValue()).getProportion().containsKey(selectNode.getComponentID())) {

                ServiceNode node = serviceNodes.get(r.getKey());
                double loss = (sourceNode.getTupleEmitRateOnSQ() * selectNode.getRatio() - lambda);// *
                double[] coeffs =selectivityFunctions.get(selectNode.getComponentID());
                double currLambda = node.getLambda();
                for (int j=0; j<coeffs.length;j++) {
                    currLambda -= (Math.pow(loss,j) * coeffs[j]);
                }
                currLambda *= ((RevertRealLoadData)r.getValue()).getProportion().get(selectNode.getComponentID());
                //System.out.println("selectNode LAMBDA:"+(sourceNode.getTupleEmitRateOnSQ() * selectNode.getRatio())+" lambda:"+lambda);
                //System.out.println(node.getComponentID()+" node lambda:"+node.getLambda()+"loss:"+loss+"currLambda:"+currLambda+" wangyihuyu: "+((RevertRealLoadData)r.getValue()).getProportion());
                if (currLambda < 0) {
                    node.changeLambdaAndOtherRelateParam(0, node.getExArrivalRate());
                } else {
                    node.changeLambdaAndOtherRelateParam(currLambda, node.getExArrivalRate());
                }
                updateServiceNode(sourceNode, serviceNodes, currLambda, node, revertRealLoadDatas, selectivityFunctions);
            }
        }

    }

    static double binarySearchBoltAfterShedLambda(ServiceNode selectNode, SourceNode sourceNode, Map<String, ServiceNode> serviceNodes, double completeTimeMilliSecUpper, double tolerant,
                                                  int totalResourceCount, double[] adjRatioArr, Map<String,double[]> selectivityFunctions, Map<String, RevertRealLoadData> revertRealLoadDatas) {
        double originLambda0 = sourceNode.getTupleEmitRateOnSQ();
        Map<String, Integer> tempAllocation;
        double estimateTSecs;
        double highMark = selectNode.getLambda();
        double lowMark = 0.0;
        double lambda = (lowMark + highMark) / 2.0;
        double load = selectNode.getExArrivalRate();
        while (lambda>1.0 && lambda>lowMark && highMark>lambda) {
            serviceNodes.get(selectNode.getComponentID()).changeLambdaAndOtherRelateParam(lambda, load);
            updateServiceNode(sourceNode, serviceNodes, lambda, selectNode, revertRealLoadDatas, selectivityFunctions);
            tempAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes,totalResourceCount);
            if (tempAllocation != null) {
                estimateTSecs = getExpectedTotalSojournTimeForJacksonOQN(serviceNodes,tempAllocation);
                double adjEstimateTMilliSec = fitEstimateRatio(estimateTSecs, adjRatioArr);
                if (adjEstimateTMilliSec < completeTimeMilliSecUpper && Math.abs(adjEstimateTMilliSec - completeTimeMilliSecUpper) <= (1.0-tolerant) * completeTimeMilliSecUpper) {
                    break;
                } else {
                    if (adjEstimateTMilliSec > completeTimeMilliSecUpper || adjEstimateTMilliSec < 0) {
                        highMark = lambda;
                    } else {
                        lowMark = lambda;
                    }
                }
            } else {
                highMark = lambda;
            }
            lambda = (lowMark + highMark) / 2.0;
            for (ServiceNode serviceNode : serviceNodes.values()) {
                serviceNode.changeLambdaAndOtherRelateParam(originLambda0 * serviceNode.getRatio(), originLambda0);
            }
        }
//        for (ServiceNode serviceNode : serviceNodes.values()) {
//            serviceNode.changeLambdaAndOtherRelateParam(originLambda0 * serviceNode.getRatio(), originLambda0);
//        }
        return lambda;
    }

    /**
     * select the bolt that have highest latency for shedding.
     * */
    private static AllocationAndActiveShedRatios greedyShedding(boolean flag, SourceNode sourceNode, Map<String, ServiceNode> serviceNodes, int totalResourceCount, double completeTimeMilliSecUpper,
                                                               double tolerant, double[] adjRatioArr, Map<String, Integer> minAllo, Map<String,double[]> selectivityFunctions, Map<String, RevertRealLoadData> revertRealLoadDatas) {
        ServiceNode selectNode = null;
        double originLambda0 = sourceNode.getTupleEmitRateOnSQ();
        Map<String, Double> activeShedRateMap = initActiveRateMap(sourceNode, serviceNodes);
        int minCount = minAllo.size();
        int remainCount = totalResourceCount - minCount;
        //System.out.println(remainCount+"minAllo:1:"+ minAllo);
        findAllocationGeneralTopApplyMMK(remainCount, minAllo, serviceNodes);
        //System.out.println("minAllo:2:"+ minAllo);

        double maxT = 0.0;
        for (ServiceNode node : serviceNodes.values()) {
            double curr = node.getRatio() * sojournTime_MMK(node.getLambda(), node.getMu(), minAllo.get(node.getComponentID()));
            if (curr < 0 && maxT >= 0) {
                maxT = curr;
                selectNode = node;
            } else if (curr > 0 && maxT >= 0 && curr > maxT) {
                maxT = curr;
                selectNode = node;
            } else if (curr < 0 && maxT <= 0 && curr > maxT) {
                maxT = curr;
                selectNode = node;
            }
        }

        if (selectNode != null) {
            double lambda = binarySearchBoltAfterShedLambda(selectNode, sourceNode, serviceNodes, completeTimeMilliSecUpper, tolerant, totalResourceCount, adjRatioArr, selectivityFunctions, revertRealLoadDatas);
            Map<String, Integer> tempAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes, totalResourceCount);
            for (ServiceNode serviceNode : serviceNodes.values()) {
                serviceNode.changeLambdaAndOtherRelateParam(originLambda0 * serviceNode.getRatio(), originLambda0);
            }
            if (tempAllocation == null) {//lambda < 1.0 ||
                LOG.warn(lambda+"too small load on "+selectNode.getComponentID()+", DRS will not select this decision!");
                return null;
            }
            double tempShedRate = Double.valueOf(String.format("%.2f",(1.0 - ( lambda / selectNode.getLambda()))));
            activeShedRateMap.put(selectNode.getComponentID(), tempShedRate);
            return new AllocationAndActiveShedRatios(tempAllocation,activeShedRateMap);
        } else {
            LOG.warn("can not find a appropriate bolt to shedding, DRS will not select this decision!");
            return null;
        }
    }

    /**
     *  select the bolt that have mininum shedding load (lambda * shedding ratio) for shedding.
     * */
    private static AllocationAndActiveShedRatios minimizedSheddingLoad(boolean flag, SourceNode sourceNode, Map<String, ServiceNode> serviceNodes, int totalResourceCount, double completeTimeMilliSecUpper,
                                                                      double tolerant, double[] adjRatioArr, Map<String, Integer> minAllo, Map<String, RevertRealLoadData> revertRealLoadDatas, Map<String, double[]> selectivityFunctions) {
        double originLambda0 = sourceNode.getTupleEmitRateOnSQ();
        Map<String, Integer> allocation = null;
        Map<String, Double> activeShedRateMap = null;
        double maxSheddingLoad = Double.MAX_VALUE;
        int minCount = minAllo.size();
        int remainCount = totalResourceCount - minCount;
        //System.out.println(remainCount+"minAllo:11:"+ minAllo);
        findAllocationGeneralTopApplyMMK(remainCount, minAllo, serviceNodes);
        //System.out.println("minAllo:22:"+ minAllo);
        String bestSelect = "default";
        for (ServiceNode selectNode : serviceNodes.values()) {
            double lambda = binarySearchBoltAfterShedLambda(selectNode, sourceNode, serviceNodes, completeTimeMilliSecUpper, tolerant, totalResourceCount, adjRatioArr, selectivityFunctions, revertRealLoadDatas);
            Map<String, Integer> tempAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes, totalResourceCount);
            for (ServiceNode serviceNode : serviceNodes.values()) {
                serviceNode.changeLambdaAndOtherRelateParam(originLambda0 * serviceNode.getRatio(), originLambda0);
            }
            if (tempAllocation == null) {//lambda < 1.0 ||
                LOG.warn(lambda+"too small load on "+selectNode.getComponentID()+", DRS will not select this decision!");
                continue;
            }

            double tempShedRatio = Double.valueOf(String.format("%.2f",(1.0 - ( lambda / selectNode.getLambda()))));
            if (selectNode.getLambda() * tempShedRatio < maxSheddingLoad) {
            //if (selectNode.getComponentID().equals("generator")) {
                maxSheddingLoad = selectNode.getLambda() * tempShedRatio;
                activeShedRateMap = initActiveRateMap(sourceNode, serviceNodes);
                activeShedRateMap.put(selectNode.getComponentID(), tempShedRatio);
                allocation = tempAllocation;
                bestSelect = selectNode.getComponentID();
                break;
            }
        }

        if (allocation != null && activeShedRateMap != null) {
            return new AllocationAndActiveShedRatios(allocation,activeShedRateMap);
        } else {
            return null;
        }
    }

    /**
     * select the bolt that have best quality (unit shedding operate have biggest impact on complete latency) for shedding.
     * */
    private static AllocationAndActiveShedRatios bestQuality(boolean flag, SourceNode sourceNode, Map<String, ServiceNode> serviceNodes, int totalResourceCount, double completeTimeMilliSecUpper,
                                                            double tolerant, double[] adjRatioArr, Map<String, Integer> minAllo, Map<String, RevertRealLoadData> revertRealLoadDatas, Map<String, double[]> selectivityFunctions) {
        double maxDiffLatencyMilliSec = 0;
        ServiceNode selectNode = null;
        double originLambda0 = sourceNode.getTupleEmitRateOnSQ();
        Map<String, Double> activeShedRateMap = initActiveRateMap(sourceNode, serviceNodes);

        int minCount = minAllo.size();
        int remainCount = totalResourceCount - minCount;
        findAllocationGeneralTopApplyMMK(remainCount, minAllo, serviceNodes);
        boolean check = true;
        for (ServiceNode serviceNode : serviceNodes.values()) {
            double originLambda = serviceNode.getLambda();
            double lambda = originLambda * 0.1;
            if (minAllo != null) {
                double beforeShed = sojournTime_MMK(serviceNode.getLambda(), serviceNode.getMu(), minAllo.get(serviceNode.getComponentID()));
                double afterShed = sojournTime_MMK(lambda, serviceNode.getMu(), minAllo.get(serviceNode.getComponentID()));
                double result = 0.0;

                if (check && beforeShed > 0) {
                    result = (beforeShed - afterShed) * serviceNode.getRatio();
                    if (result > maxDiffLatencyMilliSec) {
                        selectNode = serviceNode;
                        maxDiffLatencyMilliSec = result;
                    }
                } else if (beforeShed < 0) {
                    result = (afterShed - beforeShed) * serviceNode.getRatio();
                    if (check) {
                        selectNode = serviceNode;
                        maxDiffLatencyMilliSec = result;
                    } else if (result < maxDiffLatencyMilliSec) {
                        selectNode = serviceNode;
                        maxDiffLatencyMilliSec = result;
                    }
                    check = false;
                }
            }
        }

        for (ServiceNode serviceNode2 : serviceNodes.values()) {
            serviceNode2.changeLambdaAndOtherRelateParam(originLambda0 * serviceNode2.getRatio(), originLambda0);
        }

        if (selectNode != null) {
            LOG.info("best select operator is " + selectNode.getComponentID());
        }
        if (selectNode != null) {
            double lambda = binarySearchBoltAfterShedLambda(selectNode, sourceNode, serviceNodes, completeTimeMilliSecUpper, tolerant, totalResourceCount, adjRatioArr, selectivityFunctions, revertRealLoadDatas);
            Map<String, Integer> tempAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes, totalResourceCount);
            for (ServiceNode serviceNode : serviceNodes.values()) {
                serviceNode.changeLambdaAndOtherRelateParam(originLambda0 * serviceNode.getRatio(), originLambda0);
            }
            if (tempAllocation == null) {//lambda < 1.0 ||
                LOG.warn(lambda+"too small load on "+selectNode.getComponentID()+", DRS will not select this decision!");
                return null;
            }
            double tempShedRate = Double.valueOf(String.format("%.2f",(1.0 - ( lambda / selectNode.getLambda()))));
            activeShedRateMap.put(selectNode.getComponentID(), tempShedRate);
            return new AllocationAndActiveShedRatios(tempAllocation,activeShedRateMap);

        } else {
            LOG.warn("can not find a appropriate bolt to shedding, DRS will not select this decision!");
            return null;
        }
    }

    private static AllocationAndActiveShedRatios searchBoltShedRatio(boolean flag, SourceNode sourceNode, Map<String, ServiceNode> serviceNodes, int totalResourceCount,
                                                                         double completeTimeMilliSecUpper, double tolerant, double[] adjRatioArr, Map<String, Integer> minAllo,
                                                                         Map<String, double[]> selectivityFunctions, Map<String, RevertRealLoadData> revertRealLoadDatas) {
        AllocationAndActiveShedRatios res;
        //if (completeTimeMilliSecUpper % 10 == 1) {
        //    res = greedyShedding(flag, sourceNode, serviceNodes, totalResourceCount, completeTimeMilliSecUpper, tolerant, adjRatioArr, minAllo, selectivityFunctions, revertRealLoadDatas);
        //} else if (completeTimeMilliSecUpper % 10 == 0) {
            res = minimizedSheddingLoad(flag, sourceNode, serviceNodes, totalResourceCount, completeTimeMilliSecUpper, tolerant, adjRatioArr, minAllo, revertRealLoadDatas, selectivityFunctions);
        //} else {
        //    res = bestQuality(flag, sourceNode, serviceNodes, totalResourceCount, completeTimeMilliSecUpper, tolerant, adjRatioArr, minAllo, revertRealLoadDatas, selectivityFunctions);
        //}
        return  res;
    }

    static AllocationAndActiveShedRatios binarySearchMinimizedSpoutShedRatio(boolean flag, SourceNode sourceNode, Map<String, ServiceNode> serviceNodes, int totalResourceCount,
                                                             double completeTimeMilliSecUpper, double tolerant, double[] adjRatioArr) {
        Map<String, Integer> tempAllocation = new HashMap<>();
        double originLambda0 = sourceNode.getTupleEmitRateOnSQ();
        Map<String, Double> activeShedRateMap = initActiveRateMap(sourceNode, serviceNodes);
        double estimateTSecs;// = getExpectedTotalSojournTimeForJacksonOQN(serviceNodes,currBoltAllocation);
        double highMark = originLambda0;
        double lowMark = 0.0;
        double lambda0 = (lowMark + highMark) / 2.0;
        while (lambda0>=1 && lambda0>lowMark && highMark>lambda0) {
            for (ServiceNode serviceNode:serviceNodes.values()) {
                serviceNode.changeLambdaAndOtherRelateParam(lambda0 * serviceNode.getRatio(), lambda0);
            }

            tempAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes,totalResourceCount);
            if (tempAllocation != null) {
                estimateTSecs = getExpectedTotalSojournTimeForJacksonOQN(serviceNodes,tempAllocation);
                double adjEstimateTMilliSec = fitEstimateRatio(estimateTSecs, adjRatioArr);
                if (adjEstimateTMilliSec < completeTimeMilliSecUpper && Math.abs(adjEstimateTMilliSec - completeTimeMilliSecUpper) <= (1.0-tolerant) * completeTimeMilliSecUpper) {
                    break;
                } else {
                    if (adjEstimateTMilliSec > completeTimeMilliSecUpper || adjEstimateTMilliSec < 0) {
                        highMark = lambda0;
                    } else {
                        lowMark = lambda0;
                    }
                }
            } else {
                highMark = lambda0;
            }
            lambda0 = (lowMark + highMark) / 2.0;
        }

        for (ServiceNode serviceNode : serviceNodes.values()) {
            serviceNode.changeLambdaAndOtherRelateParam(originLambda0 * serviceNode.getRatio(), originLambda0);
        }

        double tempShedRate = Double.valueOf(String.format("%.2f",(1.0 - ( lambda0 / originLambda0))));
        if (tempShedRate >= SHED_RATIO_UNIT && tempAllocation != null) {
            activeShedRateMap.put(sourceNode.getComponentID(), tempShedRate);
        } else {
            LOG.info("Too small active shedding ratio on spout and DRS will not trigger active shedding!");
            return null;
        }

        return new AllocationAndActiveShedRatios(tempAllocation,activeShedRateMap);
    }
    /**
     * @param sourceNode
     * @param serviceNodes
     * @param totalResourceCount
     * @param completeTimeMilliSecUpper
     * @param tolerant
     * @param selectivityFunctions (for bolt active shedding when choice shedding location)
     * @param targets (for bolt active shedding when choice shedding location)
     * @return null if a) minReq of any component is Integer.MAX_VALUE (invalid parameter mu = 0.0)
     * b) total minReq can not be satisfied (total minReq > totalResourceCount)
     * otherwise, the Map data structure.
     */
    static AllocationAndActiveShedRatios suggestAllocationWithShedRate(SourceNode sourceNode, Map<String, ServiceNode> serviceNodes, int totalResourceCount,
                                                                         double completeTimeMilliSecUpper, double tolerant, Map<String, double[]> selectivityFunctions, Map<String, Object> targets, Map<String, Integer> currBoltAllocation, double[] adjRatioArr) {

        if (serviceNodes.values().stream().mapToDouble(ServiceNode::getMu).anyMatch(new DoublePredicate() {
            @Override
            public boolean test(double value) {
                if (value == 0.0){
                    return true;
                }
                return  false;
            }
        })) {
            LOG.warn("have a component mu = 0!");
            return null;
        }

        //double originLambda0 = sourceNode.getTupleEmitRateOnSQ();
        Map<String, Double> activeShedRateMap = initActiveRateMap(sourceNode, serviceNodes);

        Map<String, Integer> tempAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes, totalResourceCount);

        if (tempAllocation == null) {// need shedding

//            double estimateTSecs;// = getExpectedTotalSojournTimeForJacksonOQN(serviceNodes,currBoltAllocation);
//            double highMark = sourceNode.getTupleEmitRateOnSQ();
//            double lowMark = 0.0;
//            double lambda0 = (lowMark + highMark) / 2.0;
//
//            //while (lambda0 >= 1 && (highMark > lowMark)) {
//            while (lambda0 >= 1 && lambda0 > lowMark && (highMark > lambda0)) {
//                for (ServiceNode serviceNode:serviceNodes.values()) {
//                    serviceNode.changeLambdaAndOtherRelateParam(lambda0 * serviceNode.getRatio(),lambda0);
//                }
//
//                tempAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes,totalResourceCount);
//
//                if (tempAllocation != null) {
//
//                    estimateTSecs = getExpectedTotalSojournTimeForJacksonOQN(serviceNodes,tempAllocation);
//                    System.out.println("lambda0: "+lambda0+" estimateT: "+estimateTSecs+" tempallocation: "+tempAllocation);
//
//                    double adjEstimateTMilliSec = 0.0;
//                    for (int j=0; j<adjRatioArr.length; j++) {
//                        adjEstimateTMilliSec += Math.pow(estimateTSecs * 1000.0, j) * adjRatioArr[j];
//                    }
//                    System.out.println(estimateTSecs+" old adjEstimateTMilliSec = "+adjEstimateTMilliSec);
//                    //adjEstimateTMilliSec = Math.log(estimateTSecs* 1000.0)/Math.log(1.001378542);
//                    System.out.println(" timeout: "+completeTimeMilliSecUpper+"adjestmateT: "+adjEstimateTMilliSec);
//
//                    if (Math.abs(adjEstimateTMilliSec - completeTimeMilliSecUpper) <= (1.0-tolerant) * completeTimeMilliSecUpper) {
////                    if ((adjEstimateTMilliSec <= completeTimeMilliSecUpper)
////                            && ((adjEstimateTMilliSec / completeTimeMilliSecUpper) >= tolerant)) {
////                        //sourceNode.revertLambda(lambda0);
//                        //sourceNode.revertCompleteLatency(adjEstimateTMilliSec);
//                        System.out.println(lowMark + "method1 chenggong" + highMark);
//                        break;
//                    } else {
//                        if (adjEstimateTMilliSec > completeTimeMilliSecUpper || adjEstimateTMilliSec < 0) {
//                            System.out.println(lowMark + "method2 dale" + highMark);
//                            highMark = lambda0;
//
//                        } else {
//                            System.out.println(lowMark + "method3 xiaole" + highMark);
//                            lowMark = lambda0;
//                        }
//                    }
//                } else {
//                    System.out.println(lowMark+"method4 dale "+lambda0+" high: "+highMark);
//                    highMark = lambda0;
//                }
//                lambda0 = (lowMark + highMark) / 2.0;
//            }
//            System.out.println("high:"+highMark+" low:"+lowMark+" now lambda0: "+lambda0+" timeout: "+completeTimeMilliSecUpper+" rate: "+(1 - ( lambda0 / originLambda0)));
//            double tempShedRate = Double.valueOf(String.format("%.2f",(1.0 - ( lambda0 / originLambda0))));
//            activeShedRateMap.put(sourceNode.getComponentID(), tempShedRate);
//            for (ServiceNode serviceNode:serviceNodes.values()) {
//                serviceNode.changeLambdaAndOtherRelateParam(originLambda0 * serviceNode.getRatio(),lambda0);
//            }
//            return new AllocationAndActiveShedRatios(tempAllocation,activeShedRateMap);
            return binarySearchMinimizedSpoutShedRatio(true,sourceNode,serviceNodes,totalResourceCount,completeTimeMilliSecUpper,tolerant,adjRatioArr);
        } else {
            LOG.info("No need shedding!");
            return new AllocationAndActiveShedRatios(tempAllocation,activeShedRateMap);
        }
    }

    /**
     *  find best allocation with current resource (maybe can not fill the mini request).
     * */
    static void findAllocationGeneralTopApplyMMK(int remainCount, Map<String, Integer> retVal, Map<String, ServiceNode> serviceNodes) {
        for (int i = 0; i < remainCount; i++) {
            double maxDiff = Double.MIN_VALUE;
            String maxDiffCid = null;
            boolean check = true;
            for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
                //String cid = e.getKey();
                ServiceNode sn = e.getValue();
                int currentAllocated = retVal.get(e.getKey());
                double beforeAddT = sojournTime_MMK(sn.getLambda(), sn.getMu(), currentAllocated);
                double afterAddT = sojournTime_MMK(sn.getLambda(), sn.getMu(), currentAllocated + 1);
                //System.out.println("lambda:"+sn.getLambda()+"mu:"+sn.getMu()+"before:"+beforeAddT+" after:"+afterAddT+" nowallocation "+(1+currentAllocated));
                double diff;
                if (check && beforeAddT > 0) {
                    diff = (beforeAddT - afterAddT) * sn.getRatio();
                    if (diff > maxDiff) {
                        maxDiffCid = sn.getComponentID();
                        //System.out.println("beforeAddT>0,diff>maxDiff---------------"+maxDiffCid);
                        maxDiff = diff;
                    }
//                    } else if(diff < 0 && diff < maxDiff) {
//                        maxDiffCid = sn.getComponentID();
//                        //System.out.println("beforeAddT>0,diff<maxDiff---------------"+maxDiffCid);
//                        maxDiff = diff;
//                    }
                } else if (beforeAddT < 0) {
                    diff = (afterAddT - beforeAddT) * sn.getRatio();
                    if (check) {
                        maxDiffCid = sn.getComponentID();
                        //System.out.println("beforeAddT<0,init---------------"+maxDiffCid);
                        maxDiff = diff;
                        check = false;
                    } else if (diff < maxDiff) {
                        maxDiffCid = sn.getComponentID();
                        //System.out.println("beforeAddT<0,diff<maxDiff---------------"+maxDiffCid);
                        maxDiff = diff;
                    }
                }
               // System.out.println(e.getKey()+"nowallocation"+retVal+"~~~"+diff+" maxdiff="+maxDiff );
//                if (diff > maxDiff) {
//                    //System.out.println(maxDiff+"shibushia?"+diff);
//                    maxDiff = diff;
//                    maxDiffCid = cid;
//                }
            }

            if (maxDiffCid != null) {
                int newAllocate = retVal.compute(maxDiffCid, (k, count) -> count + 1);
                LOG.info((i + 1) + " of " + remainCount + ", assigned to " + maxDiffCid + ", newAllocate: " + retVal);
            } else {
                LOG.error((i + 1) + " of " + remainCount + ", assigned exception!");
                return;
            }
        }
    }

        /**
         * @param serviceNodes
         * @param totalResourceCount
         * @return null if a) minReq of any component is Integer.MAX_VALUE (invalid parameter mu = 0.0)
         * b) total minReq can not be satisfied (total minReq > totalResourceCount)
         * otherwise, the Map data structure.
         */
    static Map<String, Integer> suggestAllocationGeneralTopApplyMMK(Map<String, ServiceNode> serviceNodes, int totalResourceCount) {
        Map<String, Integer> retVal = serviceNodes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> getMinReqServerCount(e.getValue().getLambda(), e.getValue().getMu())));
        int topMinReq = retVal.values().stream().mapToInt(Integer::intValue).sum();

        LOG.debug("Apply M/M/K, resCnt: " + totalResourceCount + ", topMinReq: " + topMinReq);
        if (topMinReq <= totalResourceCount) {
            int remainCount = totalResourceCount - topMinReq;
            for (int i = 0; i < remainCount; i++) {
                double maxDiff = -1;
                String maxDiffCid = null;

                for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
                    String cid = e.getKey();
                    ServiceNode sn = e.getValue();
                    int currentAllocated = retVal.get(e.getKey());

                    double beforeAddT = sojournTime_MMK(sn.getLambda(), sn.getMu(), currentAllocated);
                    double afterAddT = sojournTime_MMK(sn.getLambda(), sn.getMu(), currentAllocated + 1);

                    double diff = (beforeAddT - afterAddT) * sn.getRatio();
                    //System.out.println("before:"+beforeAddT+" after:"+afterAddT+" nowallocation2 "+(1+currentAllocated));
                    if (diff > maxDiff) {
                        maxDiff = diff;
                        maxDiffCid = cid;
                    }
                }
                if (maxDiffCid != null) {
                    int newAllocate = retVal.compute(maxDiffCid, (k, count) -> count + 1);
                    LOG.debug((i + 1) + " of " + remainCount + ", assigned to " + maxDiffCid + ", newAllocate: " + newAllocate);
                } else {
                    LOG.debug("Null MaxDiffCid returned in " + (i + 1) + " of " + remainCount);
                    for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
                        String cid = e.getKey();
                        ServiceNode sn = e.getValue();
                        int currentAllocated = retVal.get(cid);

                        double beforeAddT = sojournTime_MMK(sn.getLambda(), sn.getMu(), currentAllocated);
                        double afterAddT = sojournTime_MMK(sn.getLambda(), sn.getMu(), currentAllocated + 1);

                        LOG.debug(cid + ", currentAllocated: " + currentAllocated
                                + ", beforeAddT: " + beforeAddT
                                + ", afterAddT: " + afterAddT);
                    }
                    return retVal;
                }
            }
        } else {
            LOG.info(String.format("topMinReq (%d) > totalResourceCount (%d)", topMinReq, totalResourceCount));
            return null;
        }
        return retVal;
    }

    /**
     * return a HashMap of allocation and active shedding rate.
     * */
    static List<AllocationAndActiveShedRatios> calcAllocationAndActiveShedRatio(SourceNode sourceNode, Map<String, ServiceNode> serviceNodes, int maxAvailable4Bolt, int currentUsedThreadByBolts,
                                                                              int minResource, double completeTimeMilliSecUpper, int reUnit, double tolerant, double[] adjRatioArr, Map<String,double[]> selectivityFunctions,
                                                                              Map<String, RevertRealLoadData> revertRealLoadDatas) {
        /** not make sense , we do not need triple loop
         * **/
        List<AllocationAndActiveShedRatios> AllocationAndActiveShedRatiosList = new ArrayList<>();
        Map<String, Integer> minAllo = new HashMap<>();
        int i = minResource;//currentUsedThreadByBolts;//
        AllocationAndActiveShedRatios decisionSpout;
        AllocationAndActiveShedRatios decisionBolt;
        for (; i<=maxAvailable4Bolt; i += reUnit) {
            for (String node : serviceNodes.keySet()) {
                minAllo.put(node,1);
            }
            Map<String, Integer> tempAlloResult = new HashMap<>();
            tempAlloResult.putAll(minAllo);
            int remainCount = i - minAllo.values().stream().mapToInt(Number::intValue).sum();
            Map<String, Integer> checkAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes,i);
            if (checkAllocation == null) {
                findAllocationGeneralTopApplyMMK(remainCount, tempAlloResult, serviceNodes);
            } else {
                tempAlloResult.putAll(checkAllocation);
            }
            double estT = getExpectedTotalSojournTimeForJacksonOQN(serviceNodes, tempAlloResult);
            double adjEstT = fitEstimateRatio(estT, adjRatioArr);
            if (checkAllocation != null && tempAlloResult != null && adjEstT > 0.0 && adjEstT < completeTimeMilliSecUpper) {
                decisionSpout = new AllocationAndActiveShedRatios(tempAlloResult, initActiveRateMap(sourceNode, serviceNodes));
                decisionBolt = null;
            } else {
                decisionSpout = binarySearchMinimizedSpoutShedRatio(false, sourceNode, serviceNodes, i, completeTimeMilliSecUpper, tolerant, adjRatioArr);
                decisionBolt = searchBoltShedRatio(false, sourceNode, serviceNodes, i, completeTimeMilliSecUpper, tolerant, adjRatioArr, minAllo, selectivityFunctions, revertRealLoadDatas);
            }

            if (decisionSpout != null) {
                AllocationAndActiveShedRatiosList.add(decisionSpout);
            }

            if (decisionBolt != null) {
                AllocationAndActiveShedRatiosList.add(decisionBolt);
            }
        }
        return AllocationAndActiveShedRatiosList;
    }

    static AllocationAndActiveShedRatios chooseBefittingDecision(List<AllocationAndActiveShedRatios> AllocationAndActiveShedRatiosList, ICostFunction costFunction, String costClassName, int systemModel) {
        AllocationAndActiveShedRatios result = null;
        AbstractTotalCost minCost = null;

        if (AllocationAndActiveShedRatiosList.size() > 0) {
            for (AllocationAndActiveShedRatios decision : AllocationAndActiveShedRatiosList) {
                AbstractTotalCost tempCost = costFunction.calcCost(decision);
                LOG.info("drs calc result : " + decision +" and it`s cost is "+tempCost);

                long start = Long.valueOf(jedis.get("time"));
                if (start >= 0) {//start > 54999 && start < 100000
                    systemModel = 1;
                } else {
                    systemModel = 0;
                }
                if ( result == null || minCost.compareTo(tempCost) > 0) {
                    minCost = tempCost;
                    result = decision;
                } else if (minCost.compareTo(tempCost) == 0 && systemModel == 1) { //accuracy sensitive
                    double tempResource = decision.getActiveShedRates().values().stream().mapToDouble(Number::doubleValue).sum();
                    double resResource = result.getActiveShedRates().values().stream().mapToDouble(Number::doubleValue).sum();
                    if (resResource > tempResource) {
                        minCost = tempCost;
                        result = decision;
                    }
                } else if (minCost.compareTo(tempCost) == 0 && systemModel == 0) {//cost sensitive
                    double tempRatio = decision.getActiveShedRates().values().stream().mapToDouble(Number::doubleValue).sum();
                    double resRatio = result.getActiveShedRates().values().stream().mapToDouble(Number::doubleValue).sum();
                    if (resRatio > tempRatio) {
                        minCost = tempCost;
                        result = decision;
                    }
                }
            }
            LOG.info("The best result is: " + result);
        }
        return result;
    }

    private AllocationAndActiveShedRatios getAllocationAndShedRatioGeneralTopApplyMMK(SourceNode sourceNode, Map<String, ServiceNode> serviceNodes, double estTotalSojournTimeMilliSec_MMK, int resourceUnit,
                                                                                    double completeTimeMilliSecUpper, double completeTimeMilliSecLower, int currentUsedThreadByBolts, int maxAvailable4Bolt,
                                                                                    double tolerant, Map<String, Integer> currOptAllocation, double[] adjRatioArr, Map<String,double[]> selectivityFunctions,
                                                                                    Map<String, RevertRealLoadData> revertRealLoadDatas, ICostFunction costFunction, String costClassName, int systemModel) {

        double lowerBoundServiceTimeSeconds = 0.0;  //in seconds
        int totalMinReq = 0;
        int minResource = 0;
        for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
            double lambda = e.getValue().getLambda();
            double mu = e.getValue().getMu();
            totalMinReq += getMinReqServerCount(lambda, mu);
            lowerBoundServiceTimeSeconds += (1.0 / mu);
            minResource++;
        }

        Map<String, Integer>  minPossibleAllocation = serviceNodes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> getMinReqServerCount(e.getValue().getLambda(), e.getValue().getMu())));
        int totalMinReq2 = minPossibleAllocation.values().stream().mapToInt(Integer::intValue).sum();

        if (totalMinReq != totalMinReq2) {
            LOG.warn(" getAllocationAndShedRateGeneralTopApplyMMK(), totalMinReq (" + totalMinReq + ") != totalMinReq2 (" + totalMinReq2 + ").");
        }

        double adjLowerBoundServiceTimeMilliSeconds = fitEstimateRatio(lowerBoundServiceTimeSeconds, adjRatioArr);
        LOG.debug(" getAllocationAndShedRateGeneralTopApplyMMK(), " + "lowerBoundServiceTimeSeconds * adjRatio * 1000.0 < completeTimeMilliSecUpper");
        List<AllocationAndActiveShedRatios> allocationAndActiveShedRatiosList = calcAllocationAndActiveShedRatio(sourceNode,serviceNodes,
                maxAvailable4Bolt, currentUsedThreadByBolts, minResource, completeTimeMilliSecUpper, resourceUnit, tolerant, adjRatioArr, selectivityFunctions, revertRealLoadDatas);
        return chooseBefittingDecision(allocationAndActiveShedRatiosList, costFunction, costClassName, systemModel);
    }

    /**
     * Like Module A', input required QoS, output #threads required
     * Here we separate to two modules: first output allocation, then calculate total #threads included.
     * Caution all the computation involved is in second unit.
     *
     * @param realLatencyMilliSeconds
     * @param estTotalSojournTimeMilliSec_MMK
     * @param serviceNodes
     * @param completeTimeMilliSecUpper
     * @param completeTimeMilliSecLower
     * @param currentUsedThreadByBolts
     * @param maxAvailableExec
     * @return null when status is INFEASIBLE; or FEASIBLE reallocation (with resource added)
     */
    static Map<String, Integer> getMinReqServerAllocationGeneralTopApplyMMK(
            double realLatencyMilliSeconds, double estTotalSojournTimeMilliSec_MMK, Map<String, ServiceNode> serviceNodes,
            double completeTimeMilliSecUpper, double completeTimeMilliSecLower, int currentUsedThreadByBolts, int maxAvailableExec, int reUnit) {

        double lowerBoundServiceTimeSeconds = 0.0;  //in seconds
        int totalMinReq = 0;
        for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
            double lambda = e.getValue().getLambda();
            double mu = e.getValue().getMu();
            ///caution, the unit should be millisecond
            lowerBoundServiceTimeSeconds += (1.0 / mu);
           // System.out.println("(1/mu) = "+(1.0/mu)+" mu="+mu);
            totalMinReq += getMinReqServerCount(lambda, mu);
        }

        double adjRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_MMK);

        Map<String, Integer> currAllocation = null;
        if (lowerBoundServiceTimeSeconds * adjRatio * 1000.0 < completeTimeMilliSecUpper && totalMinReq < maxAvailableExec) {
            LOG.debug(" getMinReqServerAllocationGeneralTopApplyMMK(), " +
                    "lowerBoundServiceTimeSeconds * adjRatio * 1000.0 < completeTimeMilliSecUpper && totalMinReq < maxAvailableExec");

            int i = currentUsedThreadByBolts + reUnit;
            for (; i <= maxAvailableExec; i += reUnit) {
                currAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes, i);
                double currTimeSecs = getExpectedTotalSojournTimeForJacksonOQN(serviceNodes, currAllocation);

                LOG.debug(String.format("completeT upper bound (ms): %.4f, rawCompleteTime(ms): %.4f, afterAdjust(ms): %.4f, totalMinReqQoS: %d",
                        completeTimeMilliSecUpper, currTimeSecs * 1000.0, currTimeSecs * 1000.0 * adjRatio, i));
                if (currTimeSecs * 1000.0 * adjRatio < completeTimeMilliSecUpper) {
                    break;
                }
            }

            if (i <= maxAvailableExec) {
                return currAllocation;
            }
        }
        return null;
    }

    public static Map<String, Integer> getRemovedAllocationGeneralTopApplyMMK(
            double realLatencyMilliSeconds, double estTotalSojournTimeMilliSec_MMK, Map<String, ServiceNode> serviceNodes,
            double completeTimeMilliSecUpper, double completeTimeMilliSecLower, int currentUsedThreadByBolts, int reUnit) {

        int totalMinReq2 = 0;
        for (Map.Entry<String, ServiceNode> e : serviceNodes.entrySet()) {
            double lambda = e.getValue().getLambda();
            double mu = e.getValue().getMu();
            totalMinReq2 += getMinReqServerCount(lambda, mu);
        }

        Map<String, Integer>  minPossibleAllocation = serviceNodes.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                e -> getMinReqServerCount(e.getValue().getLambda(), e.getValue().getMu())));
        int totalMinReq = minPossibleAllocation.values().stream().mapToInt(Integer::intValue).sum();

        if (totalMinReq != totalMinReq2){
            LOG.warn(" getMinReqServerAllocationGeneralTopApplyMMK(), totalMinReq (" + totalMinReq + ") != totalMinReq2 (" + totalMinReq2 + ").");
        }

        double adjRatio = realLatencyMilliSeconds / estTotalSojournTimeMilliSec_MMK;

        Map<String, Integer> currAllocation = null;
        if (currentUsedThreadByBolts > totalMinReq){
            LOG.debug(" In getRemovedAllocationGeneralTopApplyMMK(), currentUsedThreadByBolts > totalMinReq");
            int i = currentUsedThreadByBolts - reUnit;
            for (; i > totalMinReq; i = i - reUnit) {
                currAllocation = suggestAllocationGeneralTopApplyMMK(serviceNodes, i);
                double currTimeSecs = getExpectedTotalSojournTimeForJacksonOQN(serviceNodes, currAllocation);

                LOG.debug(String.format("completeT lower bound (ms): %.4f, rawCompleteTime(ms): %.4f, afterAdjust(ms): %.4f, totalMinReqQoS: %d",
                        completeTimeMilliSecLower, currTimeSecs * 1000.0, currTimeSecs * 1000.0 * adjRatio, i));
                if (currTimeSecs * 1000.0 * adjRatio > completeTimeMilliSecLower) {
                    break;
                }
            }
            if (i > totalMinReq) {
                return currAllocation;
            }
        }

        return minPossibleAllocation;
    }

    /**
     * Created by Tom Fu on Feb 21, 2017, for ToN Major revision-1, to enable resource adjustment and auto-reallocation
     * Three cases for consideration:
     * 1) resource over-provision, i.e., too much resources are used, and the real latency is far below the allowed bound
     * 2) resource shortage, i.e., resource is not enough , hence the real latency is beyond the upper-bound
     * 3) resource proper, case a) resource is just fine, only need to check whether it is in good allocation.
     * case b) the current allocation is bad, however, after reallocation to optimal allocation,
     * it will be below upper-bound
     *
     * @param realLatencyMilliSeconds
     * @param estTotalSojournTimeMilliSec_MMK
     * @param serviceNodes
     * @param completeTimeMilliSecUpper
     * @param completeTimeMilliSecLower,
     * @return AllocResult.Status
     */
    public static AllocResult.Status getStatusMMK(
            double realLatencyMilliSeconds, double estTotalSojournTimeMilliSec_MMK, double estTotalSojournTimeMilliSec_MMKOpt,
            Map<String, ServiceNode> serviceNodes, double completeTimeMilliSecUpper, double completeTimeMilliSecLower) {

        double ratio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_MMK);
        if (realLatencyMilliSeconds < completeTimeMilliSecLower) {
            return AllocResult.Status.OVERPROVISIONING;
        } else if (realLatencyMilliSeconds > completeTimeMilliSecUpper
                && ratio * estTotalSojournTimeMilliSec_MMKOpt > completeTimeMilliSecUpper) {

            //TODO: Here we conservatively include the case that the when "realLatencyMilliSeconds > completeTimeMilliSecUpper",
            //TODO: but current allocation is not the optimal one, then we will consider try optimal one before add more resources.
            return AllocResult.Status.SHORTAGE;
        }
        return AllocResult.Status.FEASIBLE;
    }

    public static AllocResult.Status getStatusMMKWithAdjRatio(
            double realLatencyMilliSeconds, double estTotalSojournTimeMilliSec_MMK, double estTotalSojournTimeMilliSec_MMKOpt,
            Map<String, ServiceNode> serviceNodes, double completeTimeMilliSecUpper, double completeTimeMilliSecLower, double[] adjRatioArr) {
        long start = Long.valueOf(jedis.get("time"));
        if (realLatencyMilliSeconds > (completeTimeMilliSecUpper*1.5)
                && estTotalSojournTimeMilliSec_MMK < 0) {
            return AllocResult.Status.SHORTAGE;
        }

        double adjEstTotalSojournTimeMilliSec_MMKOpt = fitEstimateRatio(estTotalSojournTimeMilliSec_MMKOpt / 1000.0, adjRatioArr);
        //(start > 54999 && start < 100000) &&
        if (adjEstTotalSojournTimeMilliSec_MMKOpt < completeTimeMilliSecUpper*1.5
                && realLatencyMilliSeconds < completeTimeMilliSecUpper*1.5) { // you must know that this if is no need ,it just for test.
            return AllocResult.Status.FEASIBLE;
        }

        if (realLatencyMilliSeconds < completeTimeMilliSecLower) {
            return AllocResult.Status.OVERPROVISIONING;
        } else if (realLatencyMilliSeconds > completeTimeMilliSecUpper) {
               // && adjEstTotalSojournTimeMilliSec_MMKOpt > completeTimeMilliSecUpper*1.0) {

            //TODO: Here we conservatively include the case that the when "realLatencyMilliSeconds > completeTimeMilliSecUpper",
            //TODO: but current allocation is not the optimal one, then we will consider try optimal one before add more resources.
            return AllocResult.Status.SHORTAGE;
        }
        return AllocResult.Status.FEASIBLE;
    }

    private double[] learningAdjRatio(LearningModel function) {
        double[] adjRatioArr;
        int sizeOfHistoryRatioPair = paramPairForCalcAdjRatio.historyAdjustRatioResults.size();
        if (sizeOfHistoryRatioPair >= LEARNLING_THRESHOLD) {
            Object[] objects = new Object[3];
            objects[0] = paramPairForCalcAdjRatio.historyAdjustRatioResults;
            objects[1] = ADJUST_RATIO_FUNCTION_ORDER;
            objects[2] = true;
            adjRatioArr = function.Fit(objects);

//            for (double param : adjRatioArr) {
//                    System.out.println("positive param: " + param);
//            }
        } else {
            adjRatioArr = new double[2];
            adjRatioArr[0] = 0;
            adjRatioArr[1] = 1;
        }
        return adjRatioArr;
    }
    
    @Override
    public ShedRateAndAllocResult checkOptimized(SourceNode sourceNode, Map<String, ServiceNode> queueingNetwork,
                                                 double completeTimeMilliSecUpper, double completeTimeMilliSecLower,
                                                 Map<String, Integer> currBoltAllocation, int maxAvailable4Bolt,
                                                 int currentUsedThreadByBolts, int resourceUnit) {

        ///Caution about the time unit!, second is used in all the functions of calculation
        /// millisecond is used in the output display!
        Map<String, Integer> kMaxOptAllocation = suggestAllocationGeneralTopApplyMMK(queueingNetwork, maxAvailable4Bolt);
        Map<String, Integer> currOptAllocation = suggestAllocationGeneralTopApplyMMK(queueingNetwork, currentUsedThreadByBolts);
        double estTotalSojournTimeMilliSec_MMKOpt = 1000.0 * getExpectedTotalSojournTimeForJacksonOQN(queueingNetwork, currOptAllocation);
        double estTotalSojournTimeMilliSec_MMK = 1000.0 * getExpectedTotalSojournTimeForJacksonOQN(queueingNetwork, currBoltAllocation);

        double realLatencyMilliSeconds = sourceNode.getRealLatencyMilliSeconds();
        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_MMK);
        ///relativeError (rE)
        double relativeError = Math.abs(realLatencyMilliSeconds - estTotalSojournTimeMilliSec_MMK) * 100.0 / realLatencyMilliSeconds;

        AllocResult.Status status = getStatusMMK(realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, estTotalSojournTimeMilliSec_MMKOpt,
                queueingNetwork, completeTimeMilliSecUpper, completeTimeMilliSecLower);

        Map<String, Integer> adjustedAllocation = null;
        if (status.equals(AllocResult.Status.SHORTAGE)) {
            LOG.debug("Status is resource shortage, calling resource adjustment ");
            adjustedAllocation = getMinReqServerAllocationGeneralTopApplyMMK(
                    realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, queueingNetwork,
                    completeTimeMilliSecUpper, completeTimeMilliSecLower, currentUsedThreadByBolts, maxAvailable4Bolt, resourceUnit);
            if (adjustedAllocation == null){
                LOG.debug(" Status is resource shortage and no feasible re-allocation solution");
                status = AllocResult.Status.INFEASIBLE;
            }

        } else if (status.equals(AllocResult.Status.OVERPROVISIONING)) {
            LOG.debug("Status is resource over-provisioning");
            adjustedAllocation = getRemovedAllocationGeneralTopApplyMMK(
                    realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, queueingNetwork,
                    completeTimeMilliSecUpper, completeTimeMilliSecLower, currentUsedThreadByBolts, resourceUnit);
        }

        Map<String, Object> context = new HashMap<>();
        context.put("realLatency", realLatencyMilliSeconds);
        context.put("estMMK", estTotalSojournTimeMilliSec_MMK);
        context.put("urMMK", underEstimateRatio);
        context.put("reMMK", relativeError);

        LOG.info(String.format("realLatency(ms): %.4f, estMMK: %.4f, urMMK: %.4f, reMMK: %.4f, status: %s",
                realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, underEstimateRatio, relativeError, status.toString()));

        Map<String, Map<String,Double>> activeSheddingRateMap = initActiveRateMap(sourceNode,queueingNetwork);//active shedding

        ShedRateAndAllocResult shedRateAndAllocResult = new ShedRateAndAllocResult(status, adjustedAllocation, currOptAllocation, kMaxOptAllocation, activeSheddingRateMap,context);
        AllocResult retVal = shedRateAndAllocResult.getAllocResult();

        LOG.info("MMK, reUnit: " + resourceUnit  +  ", alloStat: " + retVal.status);
        LOG.info("MMK, currOptAllo: " + retVal.currOptAllocation);
        LOG.info("MMK, adjustAllo: " + retVal.minReqOptAllocation);
        LOG.info("MMK, kMaxOptAllo: " + retVal.kMaxOptAllocation);

        return shedRateAndAllocResult;
    }

    @Override
    public ShedRateAndAllocResult checkOptimizedWithShedding(SourceNode sourceNode, Map<String, ServiceNode> queueingNetwork,
                                                 double completeTimeMilliSecUpper, double completeTimeMilliSecLower,
                                                 Map<String, Integer> currBoltAllocation, int maxAvailable4Bolt,
                                                 int currentUsedThreadByBolts, int resourceUnit, double tolerant,
                                                 double messageTimeOut, Map<String, double[]> selectivityFunctions, LearningModel calcAdjRatioFunction,
                                                 Map<String,Object> targets, Map<String, RevertRealLoadData> revertRealLoadDatas, ICostFunction costFunction, String costClassName, int systemModel) {
        long startTime = System.currentTimeMillis();
        double activeShedRatio;
        Map<String, Map<String,Double>> activeSheddingRateMap = new HashMap<>();
        double[] adjRatioArr = learningAdjRatio(calcAdjRatioFunction);
        AllocationAndActiveShedRatios KmaxOptAllocationAndActiveShedRatios = suggestAllocationWithShedRate(sourceNode,queueingNetwork,maxAvailable4Bolt,completeTimeMilliSecUpper,tolerant,selectivityFunctions, targets, currBoltAllocation, adjRatioArr);//(messageTimeOut*1000)
        activeShedRatio = KmaxOptAllocationAndActiveShedRatios.getActiveShedRates().get(sourceNode.getComponentID());
        activeSheddingRateMap.put("KmaxActiveShedRatio",KmaxOptAllocationAndActiveShedRatios.getActiveShedRates());
        AllocationAndActiveShedRatios currOptAllocationAndActiveShedRatios = suggestAllocationWithShedRate(sourceNode,queueingNetwork,currentUsedThreadByBolts,completeTimeMilliSecUpper,tolerant,selectivityFunctions, targets, currBoltAllocation, adjRatioArr);
        activeShedRatio = activeShedRatio > currOptAllocationAndActiveShedRatios.getActiveShedRates().get(sourceNode.getComponentID()) ? activeShedRatio
                : currOptAllocationAndActiveShedRatios.getActiveShedRates().get(sourceNode.getComponentID());
        currOptAllocationAndActiveShedRatios.getActiveShedRates().put(sourceNode.getComponentID(),activeShedRatio);
        activeSheddingRateMap.put("currOptActiveShedRatio",currOptAllocationAndActiveShedRatios.getActiveShedRates());
        LOG.info("shed ratio with current allocation : "+activeShedRatio);

        Map<String, Integer> currOptAllocation = currOptAllocationAndActiveShedRatios.getFixedAllocation();
        Map<String, Integer> kMaxOptAllocation = KmaxOptAllocationAndActiveShedRatios.getFixedAllocation();

        double estTotalSojournTimeMilliSec_MMKOpt = 1000.0 * getExpectedTotalSojournTimeForJacksonOQN(queueingNetwork, currOptAllocation);
        double estTotalSojournTimeMilliSec_MMK = 1000.0 * getExpectedTotalSojournTimeForJacksonOQN(queueingNetwork, currBoltAllocation);

        double realLatencyMilliSeconds = sourceNode.getRealLatencyMilliSeconds();
        ///for better estimation, we remain (learn) this ratio, and assume that the estimated is always smaller than real.
        double underEstimateRatio = Math.max(1.0, realLatencyMilliSeconds / estTotalSojournTimeMilliSec_MMK);

        if (activeShedRatio <= 0 && estTotalSojournTimeMilliSec_MMK >= 0.0 && estTotalSojournTimeMilliSec_MMK/realLatencyMilliSeconds < DEVIATION_RATIO
                && realLatencyMilliSeconds/estTotalSojournTimeMilliSec_MMK < DEVIATION_RATIO) {
            paramPairForCalcAdjRatio.putResult(realLatencyMilliSeconds , estTotalSojournTimeMilliSec_MMK );
        }
        //relativeError (rE)
        double relativeError = Math.abs(realLatencyMilliSeconds - estTotalSojournTimeMilliSec_MMK) * 100.0 / realLatencyMilliSeconds;
        AllocResult.Status status = getStatusMMKWithAdjRatio(realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, estTotalSojournTimeMilliSec_MMKOpt,
                queueingNetwork, completeTimeMilliSecUpper, completeTimeMilliSecLower, adjRatioArr);

        AllocationAndActiveShedRatios adjustedAllocationAndShedRatio = null;
        Map<String, Integer> adjustedAllocation;
        long start = Long.valueOf(jedis.get("time"));
//        if (start > 100000 && !testflag) {
//           testflag = true;
//
//        }
//        if (testflag && start >= 54999 && status == AllocResult.Status.FEASIBLE) {
//            testflag = false;
//            status = AllocResult.Status.SHORTAGE;
//            System.out.println(testflag+"~zhongxin~"+start);
//        }//no need! tkl
        if (status != AllocResult.Status.FEASIBLE ) {// no need overprovisioning !!!tkl && status != AllocResult.Status.OVERPROVISIONING
            adjustedAllocationAndShedRatio = getAllocationAndShedRatioGeneralTopApplyMMK(sourceNode, queueingNetwork, estTotalSojournTimeMilliSec_MMK, resourceUnit,
                    completeTimeMilliSecUpper, completeTimeMilliSecLower, currentUsedThreadByBolts, maxAvailable4Bolt, tolerant, currOptAllocation, adjRatioArr,
                    selectivityFunctions, revertRealLoadDatas, costFunction, costClassName, systemModel);
        }
        if (adjustedAllocationAndShedRatio != null) {
            adjustedAllocation = adjustedAllocationAndShedRatio.getFixedAllocation();
            activeSheddingRateMap.put("adjustedActiveShedRatio",adjustedAllocationAndShedRatio.getActiveShedRates());
        } else {
            adjustedAllocation = null;
            activeSheddingRateMap.put("adjustedActiveShedRatio",null);
        }

        if (adjustedAllocation == null && status == AllocResult.Status.SHORTAGE) {
            LOG.debug("Status is resource shortage and no feasible re-allocation solution");
            status = AllocResult.Status.INFEASIBLE;
        }

//        if (status.equals(AllocResult.Status.SHORTAGE)) {
//            LOG.debug("Status is resource shortage, calling resource adjustment ");
//            //suggestAllocationWithShedRate(sourceNode,queueingNetwork,currentUsedThreadByBolts,completeTimeMilliSecUpper,relativeE,selectivityFunctions, targets);
//            adjustedAllocationAndShedRatio = getMinReqServerAllocationAndShedRateGeneralTopApplyMMK(sourceNode, estTotalSojournTimeMilliSec_MMK, queueingNetwork, resourceUnit,
//                    completeTimeMilliSecUpper, completeTimeMilliSecLower, currentUsedThreadByBolts, maxAvailable4Bolt, tolerant, currOptAllocation, adjRatioArr, costFunction);
//        //    System.out.println("simalanglanglang: "+((HashMap)queueingNetwork).toString());
//            if (adjustedAllocationAndShedRatio != null) {
//                adjustedAllocation = adjustedAllocationAndShedRatio.getFixedAllocation();
//                //activeShedRate = 1 - ((1 - activeShedRate) * (1 - adjustedAllocationAndShedRatio.getActiveShedRates().get(sourceNode.getComponentID())));
//                //adjustedAllocationAndShedRatio.getActiveShedRates().put(sourceNode.getComponentID(),activeShedRate);
//                activeSheddingRateMap.put("adjustedActiveShedRate",adjustedAllocationAndShedRatio.getActiveShedRates());
//            } else {
//                adjustedAllocation = null;
//                activeSheddingRateMap.put("adjustedActiveShedRate",null);
//            }
//            //activeShedRate = adjustedAllocationAndShedRatio.getActiveShedRates().get(sourceNode.getComponentID());
//            if (adjustedAllocation == null) {
//                LOG.debug("Status is resource shortage and no feasible re-allocation solution");
//                status = AllocResult.Status.INFEASIBLE;
//            }
//
//        } else if (status.equals(AllocResult.Status.OVERPROVISIONING)) {
//            LOG.debug("Status is resource over-provisioning");
//            adjustedAllocationAndShedRatio = getRemovedAllocationAndShedRateGeneralTopApplyMMK(
//                    realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, sourceNode, queueingNetwork,
//                    completeTimeMilliSecUpper, completeTimeMilliSecLower, currentUsedThreadByBolts, resourceUnit, adjRatioArr);
//            adjustedAllocation = adjustedAllocationAndShedRatio.getFixedAllocation();
//            activeSheddingRateMap.put("adjustedActiveShedRate",adjustedAllocationAndShedRatio.getActiveShedRates());
//        }
        if (!activeSheddingRateMap.containsKey("adjustedActiveShedRatio")) { //feasible or infeasible
            activeSheddingRateMap.put("adjustedActiveShedRatio", initActiveRateMap(sourceNode, queueingNetwork));
        }

        Map<String, Object> context = new HashMap<>();
        context.put("realLatency", realLatencyMilliSeconds);
        context.put("estMMK", estTotalSojournTimeMilliSec_MMK);
        context.put("urMMK", underEstimateRatio);
        context.put("reMMK", relativeError);

        LOG.info(String.format("realLatency(ms): %.4f, estMMK: %.4f, urMMK: %.4f, reMMK: %.4f, status: %s",
                realLatencyMilliSeconds, estTotalSojournTimeMilliSec_MMK, underEstimateRatio, relativeError, status.toString()));

        ShedRateAndAllocResult shedRateAndAllocResult = new ShedRateAndAllocResult(status, adjustedAllocation, currOptAllocation, kMaxOptAllocation, activeSheddingRateMap, context);
        AllocResult retVal = shedRateAndAllocResult.getAllocResult();

        LOG.info("MMK, reUnit: " + resourceUnit  +  ", alloStat: " + retVal.status);
        LOG.info("MMK, currOptAllo: " + retVal.currOptAllocation);
        LOG.info("MMK, adjustAllo: " + retVal.minReqOptAllocation);
        LOG.info("MMK, kMaxOptAllo: " + retVal.kMaxOptAllocation);
        TestRedis.insertList("modeltime", String.valueOf(System.currentTimeMillis()-startTime));
        return shedRateAndAllocResult;
    }

    @Override
    public ShedRateAndAllocResult checkOptimizedWithActiveShedding(SourceNode sourceNode, Map<String, ServiceNode> queueingNetwork, double completeTimeMilliSecUpper, double completeTimeMilliSecLower, Map<String, Integer> currBoltAllocation, int maxAvailable4Bolt, int currentUsedThreadByBolts, int resourceUnit, double tolerant, double messageTimeOut, Map<String, double[]> selectivityFunctions, Map<String, Object> targets) {
        return null;
    }

    /**
     * if mu = 0.0 or serverCount not positive, then rho is not defined, we consider it as the unstable case (represented by Double.MAX_VALUE)
     * otherwise, return the calculation results. Leave the interpretation to the calling function, like isStable();
     *
     * @param lambda
     * @param mu
     * @param serverCount
     * @return
     */
    private static double calcRho(double lambda, double mu, int serverCount) {
        return (mu > 0.0 && serverCount > 0) ? lambda / (mu * (double) serverCount) : Double.MAX_VALUE;
    }

    /**
     * First call getRho,
     * then determine when rho is validate, i.e., rho < 1.0
     * otherwise return unstable (FALSE)
     *
     * @param lambda
     * @param mu
     * @param serverCount
     * @return
     */
    public static boolean isStable(double lambda, double mu, int serverCount) {
        return calcRho(lambda, mu, serverCount) < 1.0;
    }

    private static double calcRhoSingleServer(double lambda, double mu) {
        return calcRho(lambda, mu, 1);
    }

    public static int getMinReqServerCount(double lambda, double mu) {
        return (int) (lambda / mu) + 1;
    }

    /**
     * we assume the stability check is done before calling this function
     * The total sojournTime of an MMK queue is the sum of queueing time and expected service time (1.0 / mu).
     *
     * @param lambda,     average arrival rate
     * @param mu,         average execute rate
     * @param serverCount
     * @return
     */
    public static double sojournTime_MMK(double lambda, double mu, int serverCount) {
        //double avg = avgQueueingTime_MMK(lambda, mu, serverCount);
        //double res = avg + 1.0 / mu;
        //System.out.println("avg: "+avg+"res: "+res+"sojournTime_MMK:"+lambda+"~"+mu+"~"+serverCount);
        return avgQueueingTime_MMK(lambda, mu, serverCount) + 1.0 / mu;
    }
    /**
     * we assume the stability check is done before calling this function
     * This is a standard erlang-C formula
     *
     * @param lambda
     * @param mu
     * @param serverCount
     * @return
     */
    public static double avgQueueingTime_MMK(double lambda, double mu, int serverCount) {
        double r = lambda / (mu * (double) serverCount);
        double kr = lambda / mu;
        double phi0_p1 = 1.0;
        for (int i = 1; i < serverCount; i++) {
            double a = Math.pow(kr, i);
            double b = (double) factorial(i);
            phi0_p1 += (a / b);
        }

        double phi0_p2_nor = Math.pow(kr, serverCount);
        double phi0_p2_denor = (1.0 - r) * (double) (factorial(serverCount));
        double phi0_p2 = phi0_p2_nor / phi0_p2_denor;

        double phi0 = 1.0 / (phi0_p1 + phi0_p2);

        double pWait = phi0_p2 * phi0;

        double waitingTime = pWait * r / ((1.0 - r) * lambda);
        //System.out.println("r= "+r+" kr= "+kr+" phi0_p2_nor="+phi0_p2_nor+" phi0_p2_denor="+phi0_p2_denor+" phi0="+phi0+" phi0_p2="+phi0_p2+"factorial"+factorial(serverCount)+" pWait="+pWait+" waitingTime: "+waitingTime);
        return waitingTime;
    }

    public static double sojournTime_MM1(double lambda, double mu) {
        return 1.0 / (mu - lambda);
    }

    private static int factorial(int n) {
        if (n < 0) {
            throw new IllegalArgumentException("Attention, negative input is not allowed: " + n);
        } else if (n == 0) {
            return 1;
        } else {
            int ret = 1;
            for (int i = 2; i <= n; i++) {
                ret = ret * i;
            }
            return ret;
        }
    }

    private Map<String, Map<String,Double>> calcActiveSheddingRate() {
        Map<String, Map<String,Double>> test = new HashMap<>();
        Map<String,Double> activeSheddingRateMap = new HashMap<>();
        activeSheddingRateMap.put("sort-BoltB",Math.random());
        activeSheddingRateMap.put("sort-BoltC",Math.random());
        activeSheddingRateMap.put("sort-BoltD",0.5);
        test.put("kmax",activeSheddingRateMap);
        return test;
    }
}
