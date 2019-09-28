import org.apache.thrift.TException;
import java.util.*;


/*
  SentimentHandler is the WorkerNode handler
*/


public class SentimentHandler implements SentimentService.Iface
{
        volatile int numberOfTasks = 0;
        volatile long totalTime = 0;

        @Override
        public boolean ping() throws TException {
			       return true;
		    }

        /*
          calcFunction():
          1. Entry function of the SentimentHandler class
          2. Provides logic for both Random and Load Balanced Strategies
          3. Implements Load Injection logic
          4. Prints WorkerNode runtime statistics
        */

        @Override
        public boolean services(String str, double load, int policy) throws TException {

                // (policy == 0) : logic for Random Scheduling policy
                if(policy == 0) {
                    Random rand = new Random();
                    int l = (int)(load*10);
                    long sTime = System.currentTimeMillis();
                    mapService(str);
                    long eTime = System.currentTimeMillis();
                    synchronized(this){
                        ++numberOfTasks;
                        long execTime = eTime - sTime;
                        totalTime += execTime;
                        long currentAvg = totalTime/numberOfTasks;
                        System.out.println("File: "+str+" ,task#:"+numberOfTasks+", ran for: "+execTime+" ms,"+" current average time = "+currentAvg+" ms");
                    }
                    try{
                        int lbInj = rand.nextInt(10)+1;
                        if(lbInj <= l){
                            Thread.sleep(3000);
                        }
                    }
                    catch(Exception e){
                        System.out.println("Exception from services");
                    }
                }

                // (policy == 1) : logic for Load Balanced Scheduling policy
                else if(policy == 1) {
                  int l = (int)(load*10);
                  Random rand = new Random();
                  int n = rand.nextInt(10)+1;
                  if(n>l){
                    long sTime = System.currentTimeMillis();
                    mapService(str);
                    long eTime = System.currentTimeMillis();
                    synchronized(this){
                        ++numberOfTasks;
                        long execTime = eTime - sTime;
                        totalTime += execTime;
                        long currentAvg = totalTime/numberOfTasks;
                        System.out.println("File: "+str+" ,task#:"+numberOfTasks+", ran for: "+execTime+" ms,"+" current average time = "+currentAvg+" ms");
                    }
                    try{
                        int lbInj = rand.nextInt(10)+1;
                        if(lbInj <= l){
                            Thread.sleep(3000);
                        }
                    }
                    catch(Exception e){
                        System.out.println("Exception from services");
                    }

                    return true;
                  }
                  else{
                     System.out.println("Rejecting File : "+str+" with load probability : "+load);
                     return false;
                  }
                }
                return true;
		    }


        /*
          mapService():
          Passes the file to MapTest class where sentiment is calculated
        */

        @Override
        public void mapService(String str) throws TException {
			       try{
               MapTest mObj = new MapTest();
               mObj.entryFunction(str);
             }
             catch(Exception e){
               System.out.println(e);
             }
		    }


        /*
          sortService():
          calls the sortTest class which generates the sorted output
        */

        @Override
        public void sortService() throws TException {
          try{
            System.out.println("Got into sortService()");
            SortTest sObj = new SortTest();
            sObj.sortFunction();
          }
          catch(Exception e){
            System.out.println(e);
          }
		    }
}
