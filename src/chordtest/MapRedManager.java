/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package chordtest;

import datastructures.MapResponse;
import datastructures.Task;
import datastructures.Maps;
import datastructures.Reduce;
import datastructures.ReduceResponse;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Semaphore;
import org.apache.log4j.Logger;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import utility.PropertyLoad;
import utility.RingDownloder;

/**
 *
 * @author Ashish This Class Implements Process Manager for MapReduce task
 * consist of Queue and Semaphore to control concurrent execution.
 *
 */
public class MapRedManager implements Runnable {

    public static Map<String, Task> tasklist = new ConcurrentHashMap<String, Task>();
    public static Queue<Maps> mapQue = new LinkedList<Maps>();
    public static Queue<Reduce> reduceQue = new LinkedList<Reduce>();

    public static Map dismapSet = new ConcurrentHashMap<String, Maps>();
    public static Map disreduceSet = new ConcurrentHashMap<String, Reduce>();

    public static Semaphore tasksem = new Semaphore(1, true); //seampahore for Task Management
    public static Semaphore mapsem = new Semaphore(1, true); //Semaphore to control map que
    public static Semaphore redsem = new Semaphore(1, true);// Semaphore to control reduce que
    public static Semaphore dismapsem = new Semaphore(1, true); //Distributed map set used to know about running map task
    public static Semaphore disredsem = new Semaphore(1, true); //Distributed reduce task to know about running task
    public static Logger log = Logger.getLogger(MapRedManager.class.getName());

    public static boolean addtask(Task task) {
        //Add task in Manager every time a new client connect it's task gets added 
        try {
            tasksem.acquire();
            tasklist.put(task.appid, task);
            return true;
        } catch (Exception e) {
            System.out.println("Exception to get tasksem" + e);
            return false;
        } finally {
            tasksem.release();
        }

    }

    public static boolean addmap(List<Maps> map) {
        //Add map task in Queue of Scheduler
        try {
            mapsem.acquire();
            mapQue.addAll(map);
            return true;
        } catch (Exception e) {
            System.out.println("Exception to get mapsem" + e);
            return false;
        } finally {
            mapsem.release();
        }
    }

    public static boolean addred(Reduce reduce) {
        //Add reduce task in Queue in scheduler
        try {System.out.println("Adding Reduceres "+ reduce.appid+" "+reduce.redkey);
            redsem.acquire();
            reduceQue.add(reduce);

            return true;
        } catch (Exception e) {
            System.out.println("Exception to get redsem" + e);
            return false;
        } finally {
            redsem.release();
        }
    }

    public static boolean adddismapset(Maps map) {
        //Add map in disset when map task is sent to peer for execution
        try {
            dismapsem.acquire();
            
            dismapSet.put(map.appid + map.mapno + "map", map);
            System.out.println("Inserted in dismapset : "+map.appid + map.mapno + "map");

            return true;
        } catch (Exception e) {
            System.out.println("Exception to get dismapsem" + e);
            return false;
        } finally {
            dismapsem.release();
        }

    }

    public static boolean remdismapset(String mapid) {
        //Add map in disset when map task is sent to peer for execution
        try {
            dismapsem.acquire();
            System.out.println("Removing mapdisset"+mapid);
            dismapSet.remove(mapid);
            System.out.println(mapid);
            return true;
        } catch (Exception e) {
            System.out.println("Exception to get dismapsem" + e);
            return false;
        } finally {
            dismapsem.release();
        }

    }

    public static boolean adddisredset(Reduce red) {

        try {
            disredsem.acquire();
            System.out.println("Adding disredset: "+red.appid + red.redkey + "red");
            disreduceSet.put(red.appid + red.redkey + "red", red);
            return true;
        } catch (Exception e) {
            System.out.println("Exception to get disressem" + e);
            return false;
        } finally {
            disredsem.release();
        }

    }
    
    public static boolean remdisredset(String redid) {
        //Add map in disset when map task is sent to peer for execution
        try {
            disredsem.acquire();
            System.out.println("Removing disredset : "+redid);
            disreduceSet.remove(redid);
            System.out.println(redid);
            return true;
        } catch (Exception e) {
            System.out.println("Exception to get dismapsem" + e);
            return false;
        } finally {
            disredsem.release();
        }

    }

    public static boolean ismapQueEmpty() {
        try {
            mapsem.acquire();
            return mapQue.isEmpty();

        } catch (Exception e) {
            System.out.println("exception in mapqueempty");
            return false;
        } finally {
            mapsem.release();

        }

    }

    public static boolean isredQueEmpty() {
        try {
            redsem.acquire();
            return reduceQue.isEmpty();

        } catch (Exception e) {
            System.out.println("exception in mapqueempty");
            return false;
        } finally {
            redsem.release();

        }

    }

    public static Maps mapQuefront() {
        try {
            mapsem.acquire();
            return mapQue.peek();
        } catch (Exception e) {
            System.out.println("exception e");
            return null;
        } finally {
            mapsem.release();
        }
    }

    public static Reduce reduceQuefront() {
        try {
            redsem.acquire();
            return reduceQue.peek();
        } catch (Exception e) {
            System.out.println("exception e");
            return null;
        } finally {
            redsem.release();
        }
    }

    public static Maps removemapQue() {
        try {
            mapsem.acquire();
            return mapQue.remove();
        } catch (Exception e) {
            System.out.println("exception removemapQue");
            return null;
        } finally {
            mapsem.release();
        }
    }

    public static Reduce removeredQue() {
        try {
            redsem.acquire();
            return reduceQue.remove();
        } catch (Exception e) {
            System.out.println("exception removemapQue");
            return null;
        } finally {
            redsem.release();
        }
    }

    public static boolean mapResHandler(MapResponse mapr) {
        try {
            tasksem.acquire();
            Task t = tasklist.get(mapr.appid);
            t.cmap = t.cmap + 1;

            (t.mapkeys).addAll(mapr.keyset);
            remdismapset(mapr.appid + mapr.mapno+"map");
            if (t.cmap == t.tmap) {
                t.mapstat = true;
                t.tred = (t.mapkeys).size();

            }
            tasklist.put(t.appid, t);
            tasksem.release();
            if (t.cmap == t.tmap) {
                addReducers(t);
            }

        } catch (Exception e) {
            System.out.println(e);
        }
        return true;

    }

    public static boolean addReducers(Task t) {
        //List<String>keylist= t.mapkeys 
        for (String s : t.mapkeys) {
            Reduce r = new Reduce(t.appid, s, false);
            System.out.println("Adding Reduce task for key " + s);
            addred(r);

        }
        return true;
    }

    public static void reduceResHandler(ReduceResponse redres) {
       try {
            tasksem.acquire();
            Task t = tasklist.get(redres.appid);
            t.cred = t.cred + 1;
            boolean iscom=false;
           // (t.mapkeys).addAll(mapr.keyset);
            remdisredset(redres.appid + redres.redkey + "red");
            if (t.tred == t.cred) {
                t.redstat = true;
                iscom=true;
                t.status="Completed";
                System.out.println(t.appid + "completed");
                
                        
                
                
                //need to update results for download and clean intermediate data

            }
            tasklist.put(t.appid, t);
            tasksem.release();
            if(iscom==true)
            {
               // File resfile=new File(redres.appid+"Results");
               // RingDownloder.downloadFile(resfile.getAbsolutePath(),redres.appid+"_results");
                RingDownloder.downresults(t.appid);
            }
           

        } catch (Exception e) {
            System.out.println(e);
        }
        return;
 
    
    }

    @Override
    public void run() { //This thread listen for task in Bootstrapper
        try {
            //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            ServerSocket tsock = new ServerSocket(PropertyLoad.getInteger("tokenlistenport")); //Start Listening server for task
            while (true) {

                Socket ctsock = tsock.accept();
                TaskHandler than = new TaskHandler(ctsock);
                Thread taskth = new Thread(than);
                taskth.start(); //Start thread to handle task given by user.
            }

        } catch (Exception ex) {

            log.error("exception in MapRedmanager");
            //Logger.getLogger(MapRedManager.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
