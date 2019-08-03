/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package interfaces;

import com.siebel.data.SiebelBusComp;
import com.siebel.data.SiebelException;
import java.util.List;
import objetos.AdministracionProductos;

/**
 *
 * @author alexis.alamilla
 */
public interface DAOAdministracionProductos {
    
    public void inserta(List <AdministracionProductos> administracionProducto, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw) throws Exception;
    
    public List <AdministracionProductos> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
   
//    public void cargaBC(SiebelBusComp BC,String adminProdu)throws SiebelException;
    public void getHora();
}