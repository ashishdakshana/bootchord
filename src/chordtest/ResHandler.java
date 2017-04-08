/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chordtest;

import datastructures.MapResponse;
import datastructures.ReduceResponse;
import datastructures.TaskToken;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;
import utility.PropertyLoad;

/**
 *
 * @author Ashish
 */
public class ResHandler implements Runnable {
    
    public static ServerSocket sc;
    
    ResHandler() throws IOException
    {
        sc=new ServerSocket(PropertyLoad.getInteger("responsehandlerport"));
        
    }

    @Override
    public void run() {
        
       
            while(true){
                 try {
            Socket csock=sc.accept();
            ObjectInputStream ois=new ObjectInputStream(csock.getInputStream());
            TaskToken tsk=(TaskToken)ois.readObject();
            if(tsk instanceof MapResponse )
            {
                System.out.println("Found Map Response from "+ csock.toString());
                MapResHandler mh=new MapResHandler(csock,(MapResponse)tsk);
                new Thread(mh).start();
            }
            else if(tsk instanceof ReduceResponse )
            {
                System.out.println("Found Reduce Response from "+ csock.toString());
                RedResHandler rh=new RedResHandler(csock,(ReduceResponse)tsk);
                new Thread(rh).start();
            }
            
           // else if(tsk instace of )
                        

        } catch (IOException ex) {
            Logger.getLogger(ResHandler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(ResHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        {
        }
        
        }
        
        
         //To change body of generated methods, choose Tools | Templates.
    }
    
    
    
    
}
