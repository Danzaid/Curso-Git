/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package interfaces;

import com.siebel.data.SiebelBusComp;
import com.siebel.data.SiebelException;
import java.util.List;
import objetos.ModelosEstadostransPuestoTrabajoHijo1;
import objetos.ModelosEstadostransPuestoTrabajoHijo2;
//import objetos.ModelosEstadostransPuestoTrabajoHijo3;
import objetos.ModelosEstadostransPuestoTrabajoPadre;
import objetos.ModelosEstadostransPuestoTrabajoSoloHijo1;
import objetos.ModelosEstadostransPuestoTrabajoSoloHijo2;

/**
 *
 * @author hector.pineda
 */
public interface DAOModelosEstadostransPuestoTrabajo {
    public void inserta(List <ModelosEstadostransPuestoTrabajoPadre> listaModelosEstadostransPuestoTrabajo, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception;
    public List <ModelosEstadostransPuestoTrabajoHijo1> consultaHijo1(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public List <ModelosEstadostransPuestoTrabajoHijo2> consultaHijo2(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
//    public List <ModelosEstadostransPuestoTrabajoHijo3> consultaHijo3(String IdPadre)throws Exception;
    public List <ModelosEstadostransPuestoTrabajoPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
    public void cargaBC(SiebelBusComp BC,SiebelBusComp oBCPick,String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws SiebelException;
    public void cargaBC2(SiebelBusComp BC,SiebelBusComp BC2,SiebelBusComp BC3, SiebelBusComp oBCPick,String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws SiebelException;
//    public void cargaBC3(SiebelBusComp BC,SiebelBusComp BC2, SiebelBusComp oBCPick,String RowID, String IdPadre)throws SiebelException;
    public List <ModelosEstadostransPuestoTrabajoSoloHijo1> consultaSoloHijo1(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public List <ModelosEstadostransPuestoTrabajoSoloHijo2> consultaSoloHijo2(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
    public void getHora();
    
}
