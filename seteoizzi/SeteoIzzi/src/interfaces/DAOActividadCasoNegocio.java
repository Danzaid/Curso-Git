/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package interfaces;

import com.siebel.data.SiebelBusComp;
import com.siebel.data.SiebelException;
import java.util.List;
import objetos.ActividadCasoNegocioHijo;
import objetos.ActividadCasoNegocioPadre;
import objetos.ActividadCasoNegocioSoloHijo;
/**
 *
 * @author hector.pineda
 */
public interface DAOActividadCasoNegocio {
    public void inserta(List <ActividadCasoNegocioPadre> actividadCasoNegocio, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception;
    public List <ActividadCasoNegocioHijo> consultaHijo(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public List <ActividadCasoNegocioSoloHijo> consultaSoloHijo(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick,String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws SiebelException;//checar valor de BC o BCCh
    public void getHora();
    public List <ActividadCasoNegocioPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
}
