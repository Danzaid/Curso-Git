/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package interfaces;

import com.siebel.data.SiebelBusComp;
import com.siebel.data.SiebelException;
import java.util.List;
import objetos.GrupoPolizasHijo;
import objetos.GrupoPolizasPadre;
import objetos.GrupoPolizasSoloHijo;
/**
 *
 * @author hector.pineda
 */
public interface DAOGrupoPolizas {
    public void inserta(List <GrupoPolizasPadre> grupoPolizas, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception;
    public List <GrupoPolizasHijo> consultaHijo(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public List <GrupoPolizasSoloHijo> consultaSoloHijo(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick,String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws SiebelException;
    public void getHora();
    public List <GrupoPolizasPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
}
