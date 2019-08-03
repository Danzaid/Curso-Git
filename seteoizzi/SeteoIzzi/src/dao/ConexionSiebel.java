/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dao;

import com.siebel.data.SiebelDataBean;
import com.siebel.data.SiebelException;
import java.sql.Connection;
import java.sql.DriverManager;
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
public class ConexionSiebel extends Ejecuta_Todo {

    protected Connection conexiones;

    public void ConexionSiebel(SiebelDataBean m_dataBean, String Conectar, String urlIn, String usuarioIn, String passIn) throws SiebelException, SQLException {

        if ("SI".equals(Conectar)) {

            try {
                m_dataBean.login(urlIn, usuarioIn, passIn, "ESN");
                System.out.println("Conectado a ambiente Siebel destino: " + Ejecuta_Todo.AmbienteI);

                String mensaje = "Conectado a ambiente Siebel destino: " + Ejecuta_Todo.AmbienteI;
                Reportes rep = new Reportes();
                rep.agregarTextoAlfinal(mensaje);
            } catch (Exception e) {

                String error = e.getMessage();
                System.out.println("Error al conectar a Siebel destino: " + error);

            }

        }
    }

    public Map<String, String> ConexionSiebelPrueba(SiebelDataBean m_dataBean, String Conectar, String urlIn, String usuarioIn, String passIn) {
        Map<String, String> map = new HashMap();

        if ("SI".equals(Conectar)) {

            try {
                m_dataBean.login(urlIn, usuarioIn, passIn, "ESN");
                map.put("validacion", "SI");
                map.put("Error", "Correcto");
            } catch (SiebelException ex) {
                map.put("validacion", "NO");
                map.put("Error", ex.getMessage());

            }

        }
        return map;
    }

    public void CloseSiebel(SiebelDataBean m_dataBean) throws SiebelException {
        Ejecuta_Todo prin = new Ejecuta_Todo();

        String Conexion = "";
        try {
            if (m_dataBean != null) {
                m_dataBean.logoff();
                System.out.println("Conexion Siebel " + Ejecuta_Todo.AmbienteI + " Cerrado");
                String mensaje = "Conexion Siebel " + Ejecuta_Todo.AmbienteI + " Cerrado";
                Reportes rep = new Reportes();
                rep.agregarTextoAlfinal(mensaje);
            }
        } catch (SiebelException e) {
            throw e;
        }
    }

}
