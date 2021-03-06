package resa.shedding.example;

import resa.shedding.basicServices.api.AbstractTotalCost;
import resa.shedding.basicServices.api.AllocationAndActiveShedRatios;
import resa.shedding.basicServices.api.ICostFunction;

import java.util.Map;

/**
 * Created by 44931 on 2017/9/27.
 */
public class FPAndODCostFunc implements ICostFunction {

    @Override
    public AbstractTotalCost calcCost(Map conf, AllocationAndActiveShedRatios args) {
        double alloCost = args.getFixedAllocation().values().stream().mapToDouble(Number::doubleValue).sum();
        double shedCost = args.getActiveShedRatios().values().stream().mapToDouble(Number::doubleValue).sum();
        return new FPAndODCost(conf, alloCost, shedCost);
    }
}
