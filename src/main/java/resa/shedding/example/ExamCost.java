package resa.shedding.example;

import resa.shedding.basicServices.api.AbstractTotalCost;

import java.util.Map;

/**
 * Created by 44931 on 2017/8/7.
 */
public class ExamCost extends AbstractTotalCost {

    public ExamCost(Map conf, double resourceCost, double shedCost) {
        super(conf, resourceCost, shedCost);
    }

    @Override
    public double calcTotalCost(double resourceCost, double shedCost) {
        double cost = resourceCost+shedCost;
        System.out.println("examplecost:" +cost);
        return cost;
    }
}
