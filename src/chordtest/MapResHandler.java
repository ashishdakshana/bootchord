/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chordtest;

import datastructures.MapResponse;
import java.net.Socket;

/**
 *
 * @author Ashish
 */
public class MapResHandler implements Runnable{
     private Socket sc;
     private MapResponse mres;
     
    public MapResHandler(Socket sc,MapResponse mres) {
        this.sc=sc;
        this.mres=mres;
    }

    @Override
    public void run() {
        
        MapRedManager.mapResHandler(mres);

        }
    
    
    
    
}
