package resa.shedding.example;

import resa.shedding.basicServices.api.AbstractTotalCost;
import resa.shedding.basicServices.api.AllocationAndActiveShedRatios;
import resa.shedding.basicServices.api.ICostFunction;

import java.util.Map;

/**
 * Created by 44931 on 2017/8/7.
 */
public class ExamCostFunc implements ICostFunction {

    @Override
    public AbstractTotalCost calcCost(Map conf, AllocationAndActiveShedRatios args) {
        double alloCost = args.getFixedAllocation().values().stream().mapToDouble(Number::doubleValue).sum();
        double shedCost = args.getActiveShedRatios().values().stream().mapToDouble(Number::doubleValue).sum();
        System.out.println(alloCost+" examplecostfunction "+shedCost);
        return new ExamCost(conf, alloCost, shedCost);
    }


}
