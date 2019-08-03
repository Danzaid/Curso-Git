/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package interfaces;

import com.siebel.data.SiebelBusComp;
import com.siebel.data.SiebelException;
import java.util.List;
import objetos.ListaPolizasHijo1;
import objetos.ListaPolizasHijo2;
import objetos.ListaPolizasHijo3;
import objetos.ListaPolizasPadre;
import objetos.ListaPolizasSoloHijo1;
import objetos.ListaPolizasSoloHijo2;

/**
 *
 * @author hector.pineda
 */
public interface DAOListaPolizas {
    public void inserta(List <ListaPolizasPadre> listaPolizas, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception;
    public List <ListaPolizasPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public List <ListaPolizasHijo1> consultaHijo1(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public List <ListaPolizasHijo2> consultaHijo2(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
//    public List <ListaPolizasHijo3> consultaHijo3(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
    
    public List <ListaPolizasSoloHijo1> consultaSoloHijo1(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public List <ListaPolizasSoloHijo2> consultaSoloHijo2(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
   
    public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick,String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws SiebelException;
    public void cargaBC2(SiebelBusComp BC,SiebelBusComp BC2, SiebelBusComp oBCPick,String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws SiebelException;
//    public void cargaBC3(SiebelBusComp BC, SiebelBusComp oBCPick,String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws SiebelException;
    public void getHora();
}
