/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chordtest;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;
import utility.PropertyLoad;

/**
 *
 * @author Ashish
 * This class implements thread for client listening upon getting a connection for client 
 * it transfer control to TaskCollector to get task details and starting listening again.
 */
public class ClientListner implements Runnable {
    
    public static ServerSocket sersocket=null;
    public static String appid=null;
    public static int count=0;
    public static Logger log=Logger.getLogger(ClientListner.class.getName());
   ClientListner() throws IOException
    {
       this.sersocket=sersocket;
       sersocket=new ServerSocket(PropertyLoad.getInteger("tasklistenport"));
       
    }

    @Override
    public void run() { //Thread handle particular client for getting task details;
        log.info("Listenint for Client task : "+sersocket.toString() );
        while(true)
        {
        try {
            Socket clisocket=sersocket.accept();
            count++;
            appid=String.valueOf(count);
            
            log.info("New Client Connected for task " + clisocket.toString()+ "Generate appid :" +appid); 
            TaskCollector taskcollector=new TaskCollector();
            taskcollector.init(clisocket, appid);
            Thread t=new Thread(taskcollector);
            log.info("Starting TaskCollector for appid :"+appid);
            t.start();
            
            
            
        } catch (IOException ex) {
            Logger.getLogger(ClientListner.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        }
        }
    
    
}
