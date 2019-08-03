/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package interfaces;


import java.util.List;
import objetos.MensajesValidacion;

/**
 *
 * @author hector.pineda
 */
public interface DAOMensajesValidacion {
    public void inserta(List <MensajesValidacion> MensajesValidacion, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception;
    public List <MensajesValidacion> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
   
    public void getHora();
}
