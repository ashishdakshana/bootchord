/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datastructures;

import datastructures.TaskToken;

/**
 *
 * @author Ashish
 */
public class Maps extends TaskToken implements java.io.Serializable{

    
    
    
    public String appid;
    public int  mapno;
    public boolean status;
    public long starttime;
    
    
    public Maps(String appid,int mapno)
    {
        this.appid=appid;
        this.mapno=mapno;
        this.status=false;
        this.starttime=System.currentTimeMillis();
    }
    
    
    public String getAppid() {
        return appid;
    }

    public int getMapno() {
        return mapno;
    }

    public boolean isStatus() {
        return status;
    }
   public String toString()
    {
      return ("Appid = "+this.appid+" MapNo="+this.mapno);  
    }
    
}
