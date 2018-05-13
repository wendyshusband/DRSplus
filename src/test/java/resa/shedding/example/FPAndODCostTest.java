package resa.shedding.example;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FPAndODCostTest {
    @Test
    public void testExp() {
        //for (int i = 0; i < 10; i++) {
            double shedCost = 0.2; //i * 0.1;
            double res =  1.11756217 * Math.exp(-1 * 2.15941776 * shedCost) - 0.16101833;//fp
            //double res =  1.26652722 * Math.exp(-1 * 1.81615502 * shedCost) - 0.25757112;//od
            System.out.println(shedCost+"~"+res);
        //}
    }

    @Test
    public void tetsList() {
        List a = new ArrayList();
        a.add(1);
        a.add(2);
        a.add(3);
        a.add(4);
        List b = a.subList(1,a.size());
        b.forEach(e-> System.out.println(e));
    }

}