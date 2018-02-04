package resa.shedding.basicServices.api;

import java.util.Map;

/**
 * Created by kailin on 2017/8/7.
 */
public interface ICostFunction {

    AbstractTotalCost calcCost(Map conf, AllocationAndActiveShedRatios args);
}
