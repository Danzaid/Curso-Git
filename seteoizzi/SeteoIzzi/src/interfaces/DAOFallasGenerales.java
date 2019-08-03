/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package interfaces;

import com.siebel.data.SiebelBusComp;
import com.siebel.data.SiebelException;
import java.util.List;
import objetos.FallasGeneralesHijo;
import objetos.FallasGeneralesPadre;
import objetos.FallasGeneralesSoloHijo;
/**
 *
 * @author hector.pineda
 */
public interface DAOFallasGenerales {
        public void inserta(List <FallasGeneralesPadre> fallasGenerales, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception;
        public List <FallasGeneralesHijo> consultaHijo(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
        public List <FallasGeneralesSoloHijo> consultaSoloHijo(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
        public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick,String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra)throws SiebelException;
        public void getHora();
        public List <FallasGeneralesPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
}
