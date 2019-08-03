/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package interfaces;

import com.siebel.data.SiebelBusComp;
import com.siebel.data.SiebelException;
import java.util.List;
import objetos.ConjuntoReglasValidacionHijo1;
import objetos.ConjuntoReglasValidacionHijo2;
import objetos.ConjuntoReglasValidacionPadre;
import objetos.ConjuntoReglasValidacionSoloHijo1;
import objetos.ConjuntoReglasValidacionSoloHijo2;

/**
 *
 * @author hector.pineda
 */
public interface DAOConjuntoReglasValidacion {
   public void inserta(List <ConjuntoReglasValidacionPadre> conjuntoReglasValidacion, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception;
    public List <ConjuntoReglasValidacionPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public List <ConjuntoReglasValidacionHijo1> consultaHijo1(String IdPadre, String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public List <ConjuntoReglasValidacionHijo2> consultaHijo2(String IdPadre, String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
    public List <ConjuntoReglasValidacionSoloHijo1> consultaSoloHijo1(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception; 
    public List <ConjuntoReglasValidacionSoloHijo2> consultaSoloHijo2(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra)throws Exception;
    public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick,String RowID, String IdPadre, String usuario,String version,String ambienteInser,String ambienteExtra)throws SiebelException;
    public void getHora();
}
