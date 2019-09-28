import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import java.io.*;



/*
  Server class
  1. Receives input from Client
  2. Creates the main thread for Server and WorkerNode interaction
*/

public class Server {
    public static ClientServerHandler handler;
    public static CalculateSentiment.Processor processor;
    public static int portNumber = 0;

    public static void main(String [] args) {
      try{
        BufferedReader br = new BufferedReader(new FileReader("./config.txt"));
        int count = 0;
        String st;
        while ((st = br.readLine()) != null){
          if (count == 1){
              String[] strArr = st.split(" ");
              portNumber = Integer.parseInt(strArr[0]);
              break;
          }
          ++count;
        }
      }
      catch(Exception e){}
        try {
            handler = new ClientServerHandler();
            processor = new CalculateSentiment.Processor(handler);

            Runnable simple = new Runnable() {
                public void run() {
                    simple(processor);
                }
            };
            new Thread(simple).start();
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    public static void simple(CalculateSentiment.Processor processor) {
        try {
                TServerTransport serverTransport = new TServerSocket(portNumber);
                TTransportFactory factory = new TFramedTransport.Factory();
                TServer.Args args = new TServer.Args(serverTransport);
                args.processor(processor);
                args.transportFactory(factory);
                TServer server = new TSimpleServer(args);
                server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
