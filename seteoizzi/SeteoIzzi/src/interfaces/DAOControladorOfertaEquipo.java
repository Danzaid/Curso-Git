/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package interfaces;

import com.siebel.data.SiebelBusComp;
import com.siebel.data.SiebelException;
import java.util.List;
import objetos.ControladorOfertaEquipoHijo1;
import objetos.ControladorOfertaEquipoHijo2;
import objetos.ControladorOfertaEquipoPadre;
import objetos.ControladorOfertaEquipoSoloHijo1;
import objetos.ControladorOfertaEquipoSoloHijo2;

/**
 *
 * @author alexis.alamilla
 */
public interface DAOControladorOfertaEquipo {
    public void inserta(List <ControladorOfertaEquipoPadre> controladorOfertaEquipo, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception;
    public List <ControladorOfertaEquipoPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public List <ControladorOfertaEquipoHijo1> consultaHijo1(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public List <ControladorOfertaEquipoHijo2> consultaHijo2(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
    public List <ControladorOfertaEquipoSoloHijo1> consultaSoloHijo1(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public List <ControladorOfertaEquipoSoloHijo2> consultaSoloHijo2(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
    public void cargaBC(SiebelBusComp BC,SiebelBusComp BC2, SiebelBusComp oBCPick,String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws SiebelException;
    public void cargaBC2(SiebelBusComp BC, SiebelBusComp oBCPick,String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws SiebelException;
    public void getHora();
}
