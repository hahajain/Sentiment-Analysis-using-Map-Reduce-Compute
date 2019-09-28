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
  WorkerNode class
  Creates the main thread for all WorkerNode activities
*/

public class WorkerNode {
    public static SentimentHandler handler;
    public static SentimentService.Processor processor;
    public static int portNumber = 0;

    public static void main(String [] args) {
        try {
            BufferedReader br = new BufferedReader(new FileReader("./config.txt"));
            int count = 0;
            String st;
            while ((st = br.readLine()) != null){
              if (count == 1){
                  String[] strArr = st.split(" ");
                  portNumber = Integer.parseInt(strArr[1]);
                  break;
              }
              ++count;
            }
            handler = new SentimentHandler();
            processor = new SentimentService.Processor(handler);

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

    public static void simple(SentimentService.Processor processor) {
        try {
            //Create Thrift server socket
            TServerTransport serverTransport = new TServerSocket(portNumber);
            TTransportFactory factory = new TFramedTransport.Factory();

            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor).transportFactory(factory));
            server.serve();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
