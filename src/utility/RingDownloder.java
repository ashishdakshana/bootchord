/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utility;

import chordtest.MainClass;
import java.io.File;
import java.util.List;
import datastructures.*;
import de.uniba.wiai.lspi.chord.console.command.entry.Value;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

/**
 *
 * @author Ashish
 */
public class RingDownloder {
    
    /*
    public static List<File> downloadFile(String appid,String filename)
    {
        System.out.println("Downloading File " + filename);
        List<File> filelist=new ArrayList<File>();
       try{
       Set<Serializable> vs=chordtest.Chordtest.chord.retrieve(new Key(filename));
       
        File dir=new File("mapredresults"+File.separatorChar+appid);
           if(!dir.exists())
               dir.mkdirs();
        String dirpath=dir.getAbsolutePath();
           System.out.println("sizeof res"+vs.size());
       Iterator<Serializable> it=vs.iterator();
       
       
       while(it.hasNext())
       {  RingFile rs=(RingFile)it.next();
          byte arr[]=rs.filebytes;
          bos.write(new String(arr).getBytes());
        // bos.writeChars(new String(arr));
           System.out.println(new String(arr));
          //bos.write(((RingFile)it.next()).filebytes);
          // System.out.println(new String(arr));
           //fs.write(rs.toString());
          // System.out.println(arr);
          bos.write('\n');
           
           
       }
       bos.close();
        fs.close();
          
       System.out.println("File Download Complete :"+filename);
       return filelist; 
        
       }
       catch(Exception e)
       {
           System.out.println(e);
       }
       
       return filelist;
        
    }*/
    
    
    public static File downredFile(String appid,String redkey) throws Exception
    {
        File dir=new File("Reducedata"+File.separatorChar+appid);
           if(!dir.exists())
               dir.mkdirs();
        String dirpath=dir.getAbsolutePath();
        Set<Serializable> vs=chordtest.Chordtest.chord.retrieve(new Key(appid+"_"+redkey));
        
        FileOutputStream fs=new FileOutputStream(dirpath+File.separatorChar+redkey);
        Iterator<Serializable> it=vs.iterator();
       while(it.hasNext())
       {   
           fs.write(((RingFile)it.next()).filebytes);   
       }
       fs.close();
       
       return new File(dirpath+File.separatorChar+redkey);           
    }

    public static void downresults(String appid) {
       try{
        Set<Serializable> vs=chordtest.Chordtest.chord.retrieve(new Key(appid+"_results"));
       
        File dir=new File("mapredresults"+File.separatorChar+appid);
           if(!dir.exists())
               dir.mkdirs();
        
           System.out.println("sizeof res"+vs.size());
        Iterator<Serializable> it=vs.iterator();
        PrintWriter ps=new PrintWriter(dir.getAbsolutePath()+File.separatorChar+appid+"_results");
        
       while(it.hasNext())
       {  RingFile rs=(RingFile)it.next();
          byte arr[]=rs.filebytes;
          ps.println(new String(arr));
           
           
       }
       ps.close();
    }
    
    catch(Exception e)
    {
        e.printStackTrace();
    }
    }
}
