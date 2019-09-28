import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import java.util.*;
import java.io.*;


/*
  Client class
  1. Parses the names of all the files in input_dir
  2. Send the file list to calcFunction() in ClientServerHandler
*/

public class Client {
    static List<String> listOfFiles = new ArrayList<>();
    public static int portNumber = 0;
    public static void main(String [] args) {
        try{
          BufferedReader br = new BufferedReader(new FileReader("./config.txt"));
          int count = 0;
          String st;
          while ((st = br.readLine()) != null){
            if (count == 1){
                String[] strArr = st.split(" ");
                try{
                    portNumber = Integer.parseInt(strArr[0]);
                }
                catch(Exception e){
                  System.out.println("Port Number should be a valid Integer");
                  return;
                }
                break;
            }
            ++count;
          }
        }
        catch(Exception e){}

        //Create client connect.
        parseFileNames();
        try {
              TTransport  transport = new TSocket("localhost", portNumber);
              TProtocol protocol = new TBinaryProtocol(new TFramedTransport(transport));
              CalculateSentiment.Client client = new CalculateSentiment.Client(protocol);
              transport.open();
        			client.ping();
              client.calcFunction(listOfFiles);
            } catch(TException e) {}
      }
      public static void parseFileNames(){
          File file = new File("./data/input_dir");
          String[] fileList = file.list();
          for(String name:fileList){
              listOfFiles.add(name);
          }
      }
}
