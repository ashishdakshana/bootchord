/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chordtest;

import de.uniba.wiai.lspi.chord.console.command.entry.Key;
import de.uniba.wiai.lspi.chord.console.command.entry.Value;
import java.io.File;

/**
 *
 * @author Ashish
 */
public class MainClass {
    
    public static void main(String[] args) throws Exception
    {   cleanup();
        MapRedManager mrmap=new MapRedManager();  
        
        Thread th=new Thread(mrmap);
        th.start();
        System.out.println("Mapre");
        Chordtest chordtest=new Chordtest();
        chordtest.masterdaemon();
        
        /*
        Chordtest ch=new Chordtest();
        ch.masterdaemon();
        ch.initBootStrapper();*/
       
                
        
        
        
        
    }
    public static void cleanup()
    {File chorddata=new File("chorddata");
    if(chorddata.exists())
        chorddata.delete();
        
    }
    
    
}
