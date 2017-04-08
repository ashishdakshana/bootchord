/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chordtest;

import datastructures.Task;
import datastructures.Maps;
import utility.FileBreaker;
import chordvalues.FileasValue;
import datastructures.Key;
import datastructures.RingFile;
import de.uniba.wiai.lspi.chord.console.command.entry.Value;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import utility.RingUploder;

/**
 *
 * @author Ashish
 * Class for collecting inputdata and Mapreduce Code jar file
 */
public class TaskCollector implements Runnable {
    
    public String appid;
    public String host;
    public String status;
    public Socket clientsocket;
    public static Logger log=Logger.getLogger(TaskCollector.class.getName());
    TaskCollector()
    {
        status="new";
        
    }
    public void init(Socket clientsocket,String appid)
    {
        this.clientsocket=clientsocket;
        this.appid=appid;
        host=clientsocket.getInetAddress().toString();
    }
    public boolean getTaskobj() throws IOException, ClassNotFoundException
    {
        
        log.info("Receiving Taks  From " + host);
        String inputfilename=appid+".txt";
        FileOutputStream fos=new FileOutputStream(inputfilename);
        InputStream in=clientsocket.getInputStream();
        
        byte[] bytes = new byte[16*1024];

        int count;
        while ((count = in.read(bytes)) > 0) {
            fos.write(bytes, 0, count);
        }
        log.info("Input Text  Received for "+ appid);
       
        fos.close();
        fos=new FileOutputStream(appid+".jar");
        
        count=0;
        while ((count = in.read(bytes)) > 0) {
            fos.write(bytes, 0, count);
            
        }
        log.info("Jar Executable Received for "+ appid);
        in.close();
        fos.close();
        clientsocket.close();
        
       // fos.close();
        
        return true;
    }
    
    public long getfiles(Socket socket,String appid) throws Exception
    {
        File appdir=new File(appid);
        appdir.mkdir();
        
        BufferedInputStream bis = new BufferedInputStream(socket.getInputStream());
        DataInputStream dis = new DataInputStream(bis);

        int filesCount = 2; //two files input and jar files;
        File[] files = new File[filesCount];
        long fileLength=0;long inputFileLength=0;
        for(int i = 0; i < filesCount; i++)
        {
             fileLength = dis.readLong();
            String fileName = "";
            if(i==0)
            { fileName=appdir.getAbsolutePath()+File.separatorChar+appid+"_input";inputFileLength=fileLength;}
            else
            fileName=appdir.getAbsolutePath()+File.separatorChar+appid+"_mapred.jar";
            files[i] = new File(fileName);

            FileOutputStream fos = new FileOutputStream(files[i]);
            BufferedOutputStream bos = new BufferedOutputStream(fos);
            

            for(int j = 0; j < fileLength; j++) bos.write(bis.read());

            bos.close();
            fos.close();
        }
        
        
       // FileasValue fav=new FileasValue(appdir.getAbsolutePath()+File.separatorChar+appid+"_mapred.jar",appid,);
        

        dis.close();
        long nochunk=FileBreaker.breakfile(appdir.getAbsolutePath()+File.separatorChar+appid +"_input",appid);
         RingUploder.uploadFile(new ArrayList<File>(
                Arrays.asList(new File(appdir.getAbsolutePath()+File.separatorChar+appid+"_mapred.jar"))));
       
           /*   File fs=new File(appdir.getAbsolutePath()+File.separatorChar+appid+"_mapred.jar");
              RingFile rfile=new RingFile(fs.getName(),Files.readAllBytes(fs.toPath()));
              Chordtest.chord.insert(new Key(fs.getName()), rfile);
              System.out.println("File Uploded : "+fs.getName());
          */
       // Chordtest.chord.insert(new Key("hello"), new Value("xyz"));
        //Initalise maptask to add in list
        
         
         return nochunk;
        
        
        
        /*
        Set<Serializable> vs=chordtest.Chordtest.chord.retrieve(new  de.uniba.wiai.lspi.chord.console.command.entry.Key(appdir.getAbsolutePath()+File.separatorChar+appid +"_input1"));
       
        Object[] values = vs.toArray(new Object[vs.size()]);
        
        System.out.println(((chordvalues.FileasValue)values[0]).toString());*/
        //return true;
        
    }

    
    
    public boolean inittask(String appid,int nochunk)
    {
        
        
        List<Maps> tlist=new LinkedList<Maps>();
         
        System.out.println("No of Chunk "+ nochunk);
        for(int i=0;i<nochunk;i++)
        {
            Maps tmap=new Maps(appid,i);
            tlist.add(tmap);
           // MapRedManager.addmap(tmap);
            System.out.println("Adding Map to Scheduler "+ tmap.appid+"_"+tmap.mapno );
        }
        
        Task task=new Task(appid,nochunk,tlist);
        MapRedManager.addtask(task);
        System.out.println("Task Added to Manager");
        MapRedManager.addmap(tlist);
        
        
        System.out.println("Map Added to Scheduler for "+ appid);
        
        return true;
        
       
    }
    @Override
    public void run() {
        try {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
           // getTaskobj();
            long nochunk=getfiles(clientsocket,appid);
            
            System.out.println("Task Data Collected");
            
            inittask(appid,(int)nochunk);
            
        } catch (Exception ex) {
            Logger.getLogger(TaskCollector.class.getName()).log(Level.SEVERE, null, ex);
        } 
        
    
    }
    
}
