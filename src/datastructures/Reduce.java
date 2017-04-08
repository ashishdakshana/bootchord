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
public class Reduce extends TaskToken implements java.io.Serializable {
    
    
    
     public String appid;
    public String redkey;
    public boolean status;
     public long starttime;

    public Reduce(String appid, String redkey, boolean status) {
        this.appid = appid;
        this.redkey = redkey;
        this.status = status;
        this.starttime=System.currentTimeMillis();
    }
    
    public String getAppid() {
        return appid;
    }

    public String getRedkey() {
        return redkey;
    }

    public boolean isStatus() {
        return status;
    }
    public String toString()
    {
      return ("Appid = "+this.appid+" RedKey="+this.redkey);  
    }
    
    
    
}
