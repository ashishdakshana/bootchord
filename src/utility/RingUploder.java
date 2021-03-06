/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utility;

import datastructures.Key;
import datastructures.RingFile;
import chordtest.*;
import java.io.File;
import java.nio.file.Files;
import java.util.List;

/**
 *
 * @author Ashish
 * This class implements uploadFile function to upload file in Chord Ring 
 * it takes List of files and uses Chord object to insert data in chord ring
 * it stores data in form of RingFile Value implemeted in datastructure package.
 * 
 */
public class RingUploder {
    
    public static void uploadFile(List<File> list)throws Exception
    {
          for (File file:list)
          {   System.out.println("Uploding File : "+file.getName());
              RingFile rfile=new RingFile(file.getName(),Files.readAllBytes(file.toPath()));
              Chordtest.chord.insert(new Key(file.getName()), rfile);
              System.out.println("File Uploded : "+file.getName());
          }
    }
    public static void uploadFile(File rfile)throws Exception
    {         System.out.println("Uploding File : "+rfile.getName());
              RingFile rtfile=new RingFile(rfile.getName(),Files.readAllBytes(rfile.toPath()));
              Chordtest.chord.insert(new Key(rfile.getName()), rtfile);
              System.out.println("File Uploded : "+rfile.getName());
          
    }
    
}
