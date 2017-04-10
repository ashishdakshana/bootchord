/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chordtest;

import de.uniba.wiai.lspi.chord.data.URL;
import de.uniba.wiai.lspi.chord.service.*;
import java.net.MalformedURLException;
//import de.uniba.wiai.lspi.chord.com;

import de.uniba.wiai.lspi.chord.*;
import de.uniba.wiai.lspi.chord.console.command.entry.Value;
import java.io.Serializable;
import static java.lang.Thread.sleep;
import java.net.ServerSocket;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import utility.PropertyLoad;


/**
 *
 * @author Ashish
 */
public class Chordtest implements Runnable {

    public URL bootsurl = null;
    public static Chord chord = null;
    public static Logger log=Logger.getLogger(Chordtest.class.getName());
    

    public void initBootStrapper() {
        try {
            String bootsip=PropertyLoad.getString("bootstrapperip");
            Integer bootspo=PropertyLoad.getInteger("bootstrapperport");
            chord = new de.uniba.wiai.lspi.chord.service.impl.ChordImpl();
            chord.create(new URL("ocsocket://"+bootsip+":"+bootspo+"/ "));
            log.info("BootStrapper Running on " + chord.getURL());

        } catch (Exception e) {
            log.error("Could Not Initialize bootStrapper " + e);
            System.exit(1);
        }
    }

    public void masterdaemon() throws Exception {
        // TODO code application logic here
        de.uniba.wiai.lspi.chord.service.PropertiesLoader.loadPropertyFile();
        String protocol = URL.KNOWN_PROTOCOLS.get(URL.SOCKET_PROTOCOL);
        log.info("Initilasing BootStraper.....");
        initBootStrapper();
        log.info("BootStrapper Initalisation Successful.");
        log.info("Starting Server to Listen Client... Please Wait");
        //Socket used to listen to client

        ClientListner clistner = new ClientListner();
        
        log.info("Starting Client Thread");

        Thread cltherad = new Thread(clistner);

        cltherad.start();
        ResHandler rhan=new ResHandler();
        Thread rhanth=new Thread(rhan);
        rhanth.start();
        //log.info("Listening client for Task on : "+ sersock.toString());
        

    }

    @Override
    public void run() {
        try {
            masterdaemon();
        } catch (Exception ex) {
            java.util.logging.Logger.getLogger(Chordtest.class.getName()).log(Level.SEVERE, null, ex);
        }
        while (true) {
        }
        
    }

}
