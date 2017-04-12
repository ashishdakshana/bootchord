/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datastructures;

import datastructures.Maps;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Ashish
 */
public class Task {
    
    
    public String appid;
    public String status;
    public int tmap;
    public int cmap;
    public boolean mapstat;
    public boolean redstat;
    public int tred;
    public int cred;
    public  List<Maps> maplist;
    public  Set<String>mapkeys;
    public int inputpos;
    public String email;
    
    public Task(String appid,int tmap,List<Maps> maplist,String email)
    {
        this.appid=appid;
        this.maplist=maplist;
        this.tmap=tmap;
        status="new";
        cmap=0;
        mapstat=false;
        redstat=false;
        cred=0;
        mapkeys=new HashSet<String>();
        this.email=email;
        
        
    }
    public void inittask()
    {
        //To do initalise task create tmap tred maplist and so on
    }
    
    
    
}
