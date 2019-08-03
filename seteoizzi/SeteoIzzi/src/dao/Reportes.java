/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dao;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class Reportes {
//  
    public int countNumLineFile(String nombreArch){
        int numLine = 0;
        try {
            BufferedReader fich = new BufferedReader(new FileReader(nombreArch));
            String linea;
            try {
                while((linea = fich.readLine()) != null){
                    numLine++;
                }
            } catch (IOException e) {
                System.out.println("Error: "+e.getMessage());
            }
        } catch (FileNotFoundException e) {
            System.out.println("Error: "+e.getMessage());
        }
        return numLine;
    }
    
    public void agregarTextoAlfinal(String linea){
        File ruta = new File("");
        File file = new File(ruta.getAbsolutePath() + "\\ReporteSeteo.txt");
        
        try {
            FileWriter fstream = new FileWriter(file, true);
            BufferedWriter out = new BufferedWriter(fstream);
           
                
                out.newLine();
                out.write(""+linea+"");
            
            out.close();
        } catch (IOException ex) {
            System.out.println("Error: "+ex.getMessage());
        }
    }
}
