/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chordtest;

import datastructures.Maps;
import datastructures.Reduce;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import utility.PropertyLoad;

/**
 *
 * @author Ashish
 */
public class ReduceChurnController implements Runnable {
    public long expiretime;
    ReduceChurnController(){
    this.expiretime=PropertyLoad.getInteger("mapexprietime");
    }

    @Override
    public void run() {
        try {
            MapRedManager.disredsem.acquire();
            Map m = MapRedManager.disreduceSet;
            
            List<Reduce> explist=new ArrayList<Reduce>();
            for (Object entry : m.entrySet()) {
                Reduce p=(Reduce)entry;
                if(System.currentTimeMillis()-p.starttime >expiretime )
                {
                    //shows map task is expired;
                    m.remove(entry);
                    System.out.println(m+"Timeouted");
                    explist.add(new Reduce(p.appid,p.redkey,false));
                    
                }
                
            }
            MapRedManager.disredsem.release();
            for(Reduce r:explist){
            MapRedManager.addred(r);}
            
            Thread.sleep(expiretime);
            run();

        } catch (Exception e) {
            System.out.println(e);
        }

    }

}