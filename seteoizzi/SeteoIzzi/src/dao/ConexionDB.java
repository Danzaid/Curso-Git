/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import oracle.jdbc.OracleDriver;

/**
 *
 * @author Felipe Gutierrez
 */
public class ConexionDB extends Ejecuta_Todo {

    
    protected Connection conexion;
    public Boolean validar = true;
  
    
    private final String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
//    public String ambien = Ejecuta_Todo.jComboBox2.getSelectedItem().toString();

    
    public void ConectarDB(String urlEx,String usuarioEx,String passEx) throws SQLException {
       
//        String ambien = Ejecuta_Todo.AmbienteE;

        Reportes rep = new Reportes();

        conexion = DriverManager.getConnection(urlEx, usuarioEx,passEx);
        DriverManager.registerDriver(new OracleDriver());
        System.out.println("DB Abierto, conectado a ambiente base: " + Ejecuta_Todo.AmbienteE);
        String mensaje = "DB Abierto, conectado a ambiente base: " + Ejecuta_Todo.AmbienteE;
        rep.agregarTextoAlfinal(mensaje);

        validar = false;

    }

 public Map<String,String> ConectarDBPrueba(String urlEx,String usuarioEx,String passEx){
     
        Map<String,String> map = new HashMap();
        
        try{
        conexion = DriverManager.getConnection(urlEx, usuarioEx,passEx);
        DriverManager.registerDriver(new OracleDriver());
        map.put("validacion", "SI");
        map.put("Error", "Correcto");
        }catch(SQLException e){
        map.put("validacion", "NO");
        map.put("Error", e.getMessage());
        
        }
        
        return map;
       


    }  
    public void CloseDB() throws SQLException {

        if (conexion != null) {
            if (!conexion.isClosed()) {
                conexion.close();
                System.out.println("DB Ambiente Base: Cerrado");
                String mensaje = "DB Ambiente Base: Cerrado";
                Reportes rep = new Reportes();
                rep.agregarTextoAlfinal(mensaje);
            }
        }

    }

}
