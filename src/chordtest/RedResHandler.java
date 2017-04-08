/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chordtest;

import datastructures.ReduceResponse;
import java.net.Socket;

/**
 *
 * @author Ashish
 */
public class RedResHandler implements Runnable{
        
    private Socket sc;
    private ReduceResponse redres;

    public RedResHandler(Socket sc, ReduceResponse redres) {
        this.sc = sc;
        this.redres = redres;
    }
    
    @Override
    public void run() {
        
        MapRedManager.reduceResHandler(redres);
    }
    
}
