import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;


/*
  FileSentiment class provides the structure for each element
  of the PriorityQueue pq
*/

class FileSentiment{
  String fileName;
  double score;

  FileSentiment(String fileName, double score){
      this.fileName=fileName;
      this.score=score;
  }
}


/*
  SortTest class
  1. Reads the intermediate files
  2. Sorts files on the basis of sentiment score
  3. Writes the output to outputFile.txt
*/

public class SortTest {

    public static void sortFunction() throws FileNotFoundException {
        System.out.println("Got into sortFunction()");
        PriorityQueue<FileSentiment> pq = new PriorityQueue<FileSentiment>(1000,(f1,f2)->f2.score-f1.score>0.0?1:f1.score-f2.score>0.0?-1:0);
        String outputFilePath = "./data/output_dir/";
        FileOutputStream fos = new FileOutputStream(outputFilePath+"outputFile.txt");
        File file = new File("./data/intermediate_dir");
        String[] fileList = file.list();
        int count = 0;
        try{
          for(String name:fileList){
              BufferedReader br = new BufferedReader(new FileReader(file+"/"+name));
              String st;
              if(br != null) ++ count;
              while ((st = br.readLine()) != null){
                  String[] strArr = st.split("\\t");
                  FileSentiment fs = new FileSentiment(strArr[0], Double.parseDouble(strArr[1]));
                  pq.add(fs);
              }
          }

          int i=0;
          while(pq.size() != 0){
            FileSentiment temp =  pq.poll();
            String word=temp.fileName+"\t"+Double.toString(temp.score)+"\n";
            fos.write(word.getBytes());
          }
          fos.flush();
          fos.close();
        }
        catch(Exception e){ System.out.println(e);}
    }
}
