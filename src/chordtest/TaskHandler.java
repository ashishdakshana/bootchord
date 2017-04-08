/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chordtest;

import static chordtest.MapRedManager.adddismapset;
import static chordtest.MapRedManager.adddisredset;
import static chordtest.MapRedManager.ismapQueEmpty;
import static chordtest.MapRedManager.isredQueEmpty;
import static chordtest.MapRedManager.mapQuefront;
import static chordtest.MapRedManager.reduceQuefront;
import static chordtest.MapRedManager.removemapQue;
import static chordtest.MapRedManager.removeredQue;
import datastructures.EmptyTask;
import datastructures.Maps;
import datastructures.Reduce;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 *
 * @author Ashish
 * This class provide map and reduce token to peers uses Thread for each client.
 */
public class TaskHandler implements Runnable{
    
    
    public Socket ctsock;
    
    
    TaskHandler(Socket ctsock)
    {
        
        this.ctsock=ctsock;  
              
    }

    @Override
    public void run() {
        
        try{
         System.out.println("Client Connected for Task Token: " + ctsock.toString());
                if (!MapRedManager.isredQueEmpty()) {       //Check for Left Reduce Task
                    
                    Reduce rtask = reduceQuefront();
                // rtask.ctsock=ctsock;

                    MapRedManager.adddisredset(rtask);
                    MapRedManager.removeredQue();
                    ObjectOutputStream ops = new ObjectOutputStream(ctsock.getOutputStream());
                    ops.writeObject(rtask); //send reduce task in peer

                } else if (!MapRedManager.ismapQueEmpty()) {   //check for left Maps Task 
                    Maps mtask = MapRedManager.mapQuefront();
                    //mtask.ctsock=ctsock;
                    MapRedManager.adddismapset(mtask);
                    MapRedManager.removemapQue();
                    ObjectOutputStream ops = new ObjectOutputStream(ctsock.getOutputStream());
                    ops.writeObject(mtask);  //Send map token to peer
                } else {
                    ObjectOutputStream ops = new ObjectOutputStream(ctsock.getOutputStream());
                    ops.writeObject(new EmptyTask());
                    ctsock.close();
                }
        }
        catch(Exception e)
        {
            
        }
        
        
    }
    
    
}
