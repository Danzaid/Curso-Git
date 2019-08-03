/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package interfaces;

import com.siebel.data.SiebelBusComp;
import com.siebel.data.SiebelException;
import java.util.List;
import objetos.ListaValores;
import objetos.ListaValoresPadre;

/**
 *
 * @author hector.pineda
 */
public interface DAOListaValores {
    public void inserta(List <ListaValores> listaValores, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception;
    public List <ListaValores> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
    public List <ListaValoresPadre> consultaLisValores(String listValores,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;  
    public void cargaBC(SiebelBusComp BC,SiebelBusComp oBCPick,String LOV,String usuario,String version,String ambienteInser,String ambienteExtra)throws SiebelException;
    public void getHora();
}
