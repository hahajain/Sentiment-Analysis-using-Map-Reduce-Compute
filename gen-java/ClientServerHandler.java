import org.apache.thrift.TException;
import java.util.*;
import java.util.concurrent.*;
import java.lang.*;
import java.io.*;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.lang.*;


/*
class ConfigData : Provides the structure of the Configuration Items
*/
class ConfigData{
  String serverName;
  double load;

  ConfigData(String serverName, double load){
      this.serverName=serverName;
      this.load=load;
  }
}


/*
  ClientServerHandler is the handler between the Server and the WorkerNodes
*/

public class ClientServerHandler implements CalculateSentiment.Iface
{
        static int numberOfCompletes = 0;
        static int numberOfFiles = 0;
        static int numberOfServers;
        static int schedulingPolicy = 0;
        static int portNumber = 0;
        static ArrayList<ConfigData> arrList = new ArrayList<>();
        String shareString="";
        static Queue<String> queue = new ConcurrentLinkedQueue<String>();

        @Override
        public boolean ping() throws TException {
			       return true;
		    }

        /*
          calcFunction():
          1. Receives the list of files to be processed from the Client
          2. Reads the Scheduling Policy, Port, Server Details from the Config
          3. Calls mapCall()
        */

        @Override
        public void calcFunction(List<String> listOfFiles) throws TException {
              for(String name: listOfFiles){
                queue.add(name);
              }
              numberOfFiles = queue.size();
              System.out.println("Runnable Servers:");
              try{
                BufferedReader br = new BufferedReader(new FileReader("./config.txt"));
                BufferedReader br1 = new BufferedReader(new FileReader("./config.txt"));
                String st;
                int n =0;
                while ((st = br.readLine()) != null){
                    ++n;
                }
                numberOfServers = n-2;
                int count = 0;
                while ((st = br1.readLine()) != null){
                    if(count == 0){
                        schedulingPolicy = Integer.parseInt(st.trim());
                        if(schedulingPolicy<0 || schedulingPolicy>1){
                            System.out.println("Invalid Scheduling Policy Input in config.txt");
                            return;
                        }
                        ++ count;
                        continue;
                    }
                    else if (count == 1){
                        String[] strArr = st.split(" ");
                        try{
                            portNumber = Integer.parseInt(strArr[1]);
                        }
                        catch(Exception e){
                          System.out.println("Port Number should be a valid Integer");
                          return;
                        }

                        ++ count;
                    }
                    else {
                        String[] strArr = st.split(" ");
                        double prob = Double.parseDouble(strArr[1]);
                        if(prob<0.0 || prob>1.0){
                          System.out.println("Invalid Load Probability (should be between: 0.0 and 1.0)");
                          return;
                        }
                        arrList.add(new ConfigData(strArr[0], Double.parseDouble(strArr[1])));
                    }
                }
              }
              catch(Exception e){}
                for(ConfigData d : arrList){
                  System.out.println(d.serverName+" "+d.load);
                }
              mapCall();
        }


        /*
          mapCall():
          1. Prints Server Run Time Statistics
          2. Creates threads and send files to WorkerNodes for Map processing
        */

        public static void mapCall(){
            System.out.println("Starting Map Task");
            long mapStartTime = System.currentTimeMillis();
            while(true){
                Thread t1=new Thread();
                while(!queue.isEmpty()){
                    String name = queue.poll();
                    Random rand = new Random();
                    int n = rand.nextInt(numberOfServers);
                    Runnable simple = new Runnable() {
                      public void run() {
                        try {
                            TTransport transport = new TSocket(arrList.get(n).serverName, portNumber);
                            TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                            SentimentService.Client client = new SentimentService.Client(protocol);
                            transport.open();
                            double l = arrList.get(n).load;
                            boolean b = client.services(name, l, schedulingPolicy);
                            if(b == false){
                                 queue.add(name);
                            }
                            else{
                                counterMethod();
                            }
                         }
                         catch(TException e) {
                            System.out.println(e);
                        }
                      }
                    };
                    t1 = new Thread(simple);
                    t1.start();
                  }
                  if(numberOfCompletes == numberOfFiles) {
                      numberOfCompletes = 0;
                      break;
                  }
              }
              long mapEndTime = System.currentTimeMillis();
              System.out.println("Total Run Time of Map Task = "+((mapEndTime - mapStartTime) * 0.001)+" secs");
              sortCall();
          }



          /*
            counterMethod():
            Keeps a track of total number of completed tasks
          */

          public synchronized static void counterMethod(){
              ++ numberOfCompletes;
          }


          /*
            sortCall():
            Creates a thread and sends it to a designated node to run
            sort functionality on worker node
          */

          public static void sortCall() {
              Runnable simple = new Runnable() {
                public void run() {
                  try {
                      System.out.println("Starting Sort Task:");
                      long sortStartTime = System.currentTimeMillis();
                      TTransport  transport = new TSocket(arrList.get(0).serverName, portNumber);
                      TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
                      SentimentService.Client client = new SentimentService.Client(protocol);
                      transport.open();
                      client.sortService();
                      System.out.println("Sort Call End");
                      long sortEndTime = System.currentTimeMillis();
                      System.out.println("Total Run Time of Sort Task = "+((sortEndTime - sortStartTime) * 0.001)+" secs");
                      arrList.removeAll(arrList);
                   } catch(TException e) {
                      System.out.println(e);
                  }
                }
              };
              new Thread(simple).start();
          }
} // end of class ClientServerHandler.java
