/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package interfaces;

import com.siebel.data.SiebelBusComp;
import com.siebel.data.SiebelException;
import java.util.List;
import objetos.ListaPreciosHijo;
import objetos.ListaPreciosPadre;
import objetos.ListaPreciosSoloHijo;

/**
 *
 * @author Felipe Gutierrez
 */
public interface DAOListaPrecios {
    public void inserta(List <ListaPreciosPadre> listaPrecios, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception;
    public List <ListaPreciosHijo> consultaHijo(String IdPadre, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception; 
    public List <ListaPreciosSoloHijo> consultaSoloHijo(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception;
    public List <ListaPreciosPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public void cargaBC(SiebelBusComp BC,SiebelBusComp BC2, SiebelBusComp oBCPick,String RowID, String IdPadre, String usuario,String version,String ambienteInser,String ambienteExtra)throws SiebelException;
    public void getHora();
}