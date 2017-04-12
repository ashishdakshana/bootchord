/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chordtest;

import datastructures.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import utility.PropertyLoad;

/**
 *
 * @author Ashish
 */
public class MapChurnController implements Runnable {
    public long expiretime;
    MapChurnController(){
    this.expiretime=PropertyLoad.getInteger("mapexprietime");
        System.out.println("Map Expire time : "+ this.expiretime);
    }

    @Override
    public void run() {
        while(true){
        try {System.out.println("Running mapchurncontroller");
            
            MapRedManager.dismapsem.acquire();
            Map m = MapRedManager.dismapSet;
            
            List<Maps> explist=new ArrayList<Maps>();
            
            for (Object entry : m.keySet()) {
                Maps p=(Maps)(m.get(entry));
                if(System.currentTimeMillis()-p.starttime >expiretime )
                {
                    //shows map task is expired;
                    m.remove(entry);
                    System.out.println(p+ "Timeouted");
                    explist.add(new Maps(p.appid,p.mapno));
                    
                }
                
            }
            MapRedManager.dismapsem.release();
            MapRedManager.addmap(explist);
            Thread.sleep(expiretime);
            

        } catch (Exception e) {
            System.out.println(e);
        }
        }
    }

}
