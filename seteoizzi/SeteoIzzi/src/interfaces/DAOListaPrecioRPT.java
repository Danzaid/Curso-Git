/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package interfaces;

import java.util.List;
import objetos.ListaPreciosRPT;

/**
 *
 * @author Felipe Gutierrez
 */
public interface DAOListaPrecioRPT {
    public void inserta(List <ListaPreciosRPT> listaPreciosRPT,String it) throws Exception;
    public List <ListaPreciosRPT> consultaListaPreciosRPT()throws Exception;     
    public void updateRegistro(String sRowId)throws Exception;     
    public void getHora();
}
