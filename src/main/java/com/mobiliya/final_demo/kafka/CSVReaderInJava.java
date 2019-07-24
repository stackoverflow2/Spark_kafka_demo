/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package final_demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple Java program to read CSV file in Java. In this program we will read
 * list of books stored in CSV file as comma separated values.
 * 
 * @author WINDOWS 8
 *
 */
public class CSVReaderInJava {

    public static List<String> readBooksFromCSV(String fileName) {
        List<String> list = new ArrayList<>();
        Path pathToFile = Paths.get(fileName);

        try (BufferedReader br = Files.newBufferedReader(pathToFile,
                StandardCharsets.UTF_8)) {

            // read the first line from the text file
            String line = br.readLine();
                      
            // loop until all lines are read
                        
            while (line != null) {

                
                String[] attributes = line.split(",");
                
                if(attributes[2].contains("\"")){
                 String s = attributes[2].substring(1, attributes[2].length()-1) + "," + 
                            attributes[4].substring(1, attributes[4].length()-1) + "," + 
                            attributes[5].substring(1, attributes[5].length()-1) + "," +
                            attributes[7].substring(1, attributes[7].length()-1) + "," +
                            attributes[8].substring(1, attributes[8].length()-1) + "," +
                            attributes[9].substring(1, attributes[9].length()-1);
                 list.add(s);
                }else{
                 String s = attributes[2] + "," + 
                            attributes[4] + "," + 
                            attributes[5] + "," +
                            attributes[7] + "," +
                            attributes[8] + "," +
                            attributes[9];   
                 list.add(s);
                }
                //System.out.println(s);
                //list.add(s);
                // read next line before looping
                // if end of file reached, line would be null
                line = br.readLine();
            }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        return list;
    }

   

}

