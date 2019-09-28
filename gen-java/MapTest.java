import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/*
  This file performs the entire Map Task for each individual file sent to it
*/

public class MapTest {

    /* entryFunction():
          1. This is the entry function of the MapTest Class
          2. It calculates the final sentiment for each file
    */

    public static void entryFunction(String name) throws Exception {
            int pos = 0, neg = 0;
            HashSet<String> positives = createHash("./data/positive.txt", new HashSet<>());
            HashSet<String> negatives = createHash("./data/negative.txt", new HashSet<>());
            String str="./data/input_dir/"+name;
            File file = new File(str);
            BufferedReader br = new BufferedReader(new FileReader(file));
            String st;
            while ((st = br.readLine()) != null){
              st = st.replaceAll("--", "");
              st = st.replaceAll("'", " ");
              Matcher m = Pattern.compile("[a-z-]+").matcher(st.trim().toLowerCase());
              while (m.find()) {
                String temp = m.group(0);
                if(positives.contains(temp)) ++pos;
                if(negatives.contains(temp)) ++neg;
              }
            }
            double sentA = (pos - neg);
            double sentB = (pos + neg);
            double sentScore = sentA/sentB;
            writeToFile(name, sentScore);
    }


    /* createHash():
          Populates the positives and negatives HashSet using which the
          sentiment score would be evaluated
    */

    public static HashSet<String> createHash(String path, HashSet<String> hSet) throws Exception{
        File fileName = new File(path);
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String st;
        while ((st = br.readLine()) != null){
            String stP = st.trim();
            stP.toLowerCase();
            hSet.add(stP);
        }
        return hSet;
    }

    /* writeToFile():
          1. Creates an intermediate file for each fileName
          2. Writes the fileName and the computed score into the file
    */

    public static void writeToFile(String fileName, double sentScore) throws IOException{
        String filePath = "./data/intermediate_dir/";
        FileOutputStream fos = new FileOutputStream(filePath+"file"+fileName);
        String word=fileName+"\t"+sentScore;
        fos.write(word.getBytes());
        fos.flush();
        fos.close();
    }
}
