/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package interfaces;

import com.siebel.data.SiebelException;
import com.siebel.data.SiebelBusComp;
import java.util.List;
import objetos.DescuentoAgregacionHijo;
import objetos.DescuentoAgregacionPadre;
import objetos.DescuentoAgregacionSoloHijo;

/**
 *
 * @author Felipe Gutierrez
 */
public interface DAODescuentoAgregacion {
    public void inserta(List <DescuentoAgregacionPadre> accionesPolizas, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception;
    public List <DescuentoAgregacionHijo> consultaHijo(String IdPadre, String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public List <DescuentoAgregacionSoloHijo> consultaSoloHijo(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick,String RowID, String IdPadre, String usuario,String version,String ambienteInser,String ambienteExtra)throws SiebelException;
    public void getHora();
    public List <DescuentoAgregacionPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
}
