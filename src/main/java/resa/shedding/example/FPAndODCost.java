 package resa.shedding.example;

import redis.clients.jedis.Jedis;
import resa.shedding.basicServices.api.AbstractTotalCost;
import resa.shedding.tools.TestRedis;
import resa.util.ConfigUtil;

import java.util.Date;
import java.util.Map;

 /**
 * Created by 44931 on 2017/9/27.
 */
public class FPAndODCost extends AbstractTotalCost {

    private static double threshold;// = 0.6;
    private static double costThreshold;// = 21.0;//17.0;
    private static boolean testCheckSystemModel;
    private static final Jedis jedis = TestRedis.getJedis();
    private static String name = jedis.get("type");

    private static double odAccuracySensitive(double shedCost) {
        //double res = 0.93298262 * Math.exp(-1 * 4.91578576 * shedCost) + 0.06391202;
        double res =  1.26652722 * Math.exp(-1 * 1.81615502 * shedCost) - 0.25757112;
        //double res = -1.1 * shedCost + 0.87;
        System.out.println(shedCost+"accuracytianshichibangOD:"+res);
        if (res <= threshold) {
            return Double.MAX_VALUE;
        }
        return 0;
    }

    private static double fpAccuracySensitive(double shedCost) {
        //double res = -0.95625272727 * shedCost + 0.851328;
        //double res = -1 * shedCost + 0.81;
        //double res = -0.86 * shedCost + 0.71;
        //double res = -0.86 * shedCost + 0.78;
        double res =  1.11756217 * Math.exp(-1 * 2.15941776 * shedCost) - 0.16101833;
        System.out.println("accuracytianshichibangFP:"+res);
        if (res < threshold) {
            return Double.MAX_VALUE;
        }
        return 0;
    }

    private static double costSensitive(double resourceCost, double shedCost) {
        double cost = resourceCost + 100 * shedCost;
        if (resourceCost > costThreshold) {
            return Double.MAX_VALUE;
        } else {
            return cost;
        }
    }

    public FPAndODCost(Map conf, double resourceCost, double shedCost) {
        super(conf, resourceCost, shedCost);
        threshold = ConfigUtil.getDouble(conf, "accuracy.threshold", 0.6);
        costThreshold = ConfigUtil.getDouble(conf, "cost.threshold", 60.0);
        int temp = ConfigUtil.getInt(conf, "resa.shedding.system.model", 0);
        if (temp == 0) {
            testCheckSystemModel = false;
        } else {
            testCheckSystemModel = true;
        }
        System.out.println("FPAndODCost! "+threshold+" /// "+costThreshold+" systemmodel: "+testCheckSystemModel);
    }

    @Override
    public double calcTotalCost(double resourceCost, double shedCost) {
        double cost;
        long start = Long.valueOf(jedis.get("time"));
        Date date = new Date();
        if (start > 85999 && start < 146000) {//start > 39999 && start < 70000　testCheckSystemModel
        //if (testCheckSystemModel) {
            System.out.println(name+" wtf:accuracysensitive"+start+"now:"+date.toString());
            if (name.equals("od")) {
                cost = resourceCost + odAccuracySensitive(shedCost);
            } else {
                cost = resourceCost + fpAccuracySensitive(shedCost);
            }
        } else {
            System.out.println(name+" wtf:costsensitive"+start+"now"+date.toString());
            cost = costSensitive(resourceCost,shedCost);
        }
        return cost;
    }
}
