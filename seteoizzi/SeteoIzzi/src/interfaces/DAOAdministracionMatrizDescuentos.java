/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package interfaces;

import java.util.List;
import objetos.AdministracionMatrizDescuentos;


/**
 *
 * @author alexis.alamilla
 */
public interface DAOAdministracionMatrizDescuentos {

    public List <AdministracionMatrizDescuentos> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
    public void inserta(List <AdministracionMatrizDescuentos> administracionMatrizDescuentos, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception;
       
     
}
