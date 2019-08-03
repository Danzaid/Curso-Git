/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package interfaces;

import com.siebel.data.SiebelBusComp;
import com.siebel.data.SiebelException;
import java.util.List;
import objetos.MatricesElegibilidadCompHijo1;
import objetos.MatricesElegibilidadCompHijo2;
import objetos.MatricesElegibilidadCompPadre;
import objetos.MatricesElegibilidadCompSoloHijo1;
import objetos.MatricesElegibilidadCompSoloHijo2;

/**
 *
 * @author hector.pineda
 */
public interface DAOMatricesElegibilidadComp {
    public void inserta(List <MatricesElegibilidadCompPadre> matricesElegibilidadComp,String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception;
    public List <MatricesElegibilidadCompPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public List <MatricesElegibilidadCompHijo1> consultaHijo1(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public List <MatricesElegibilidadCompHijo2> consultaHijo2(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
    public List <MatricesElegibilidadCompSoloHijo1> consultaSoloHijo1(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public List <MatricesElegibilidadCompSoloHijo2> consultaSoloHijo2(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
    public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick,String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws SiebelException;
    public void cargaBC2(SiebelBusComp BC, SiebelBusComp oBCPick, String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws SiebelException;
    public void getHora();
    
}
