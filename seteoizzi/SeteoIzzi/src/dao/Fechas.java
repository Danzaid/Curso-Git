    /*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dao;

import com.toedter.calendar.JDateChooser;
import java.text.SimpleDateFormat;
/**
 *
 * @author hector.pineda
 */
public class Fechas {

     SimpleDateFormat Formato = new SimpleDateFormat("MM/dd/yyyy");

    public String fechaInicio(JDateChooser fechaI) {
        if (fechaI.getDate() != null) {
            return Formato.format(fechaI.getDate());
        } else {
            return null;
        }   
    }
    
    SimpleDateFormat Formate = new SimpleDateFormat("MM/dd/yyyy");

    public String fechaTermino(JDateChooser fechaT) {
        if (fechaT.getDate() != null) {
            return Formate.format(fechaT.getDate());
        } else {
            return null;
        }
    }
    
    
    
    
    
    public static void main(String[] args) throws Exception{
        
        
    }
}
