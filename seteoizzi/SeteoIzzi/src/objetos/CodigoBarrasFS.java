/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package objetos;

/**
 *
 * @author alexis.alamilla
 */
public class CodigoBarrasFS {//corregido
    private String nombreVista;
    private String nombreApplet;
    private String nombreCampo;
    private String modo;
    private String modoProceso;
    private String rowId;

    public String getRowId() {  
        return rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }

    public String getNombreVista() {
        return nombreVista;
    }

    public void setNombreVista(String nombreVista) {
        this.nombreVista = nombreVista;
    }

    public String getNombreApplet() {
        return nombreApplet;
    }

    public void setNombreApplet(String nombreApplet) {
        this.nombreApplet = nombreApplet;
    }

    public String getNombreCampo() {
        return nombreCampo;
    }

    public void setNombreCampo(String nombreCampo) {
        this.nombreCampo = nombreCampo;
    }

    public String getModo() {
        return modo;
    }

    public void setModo(String modo) {
        this.modo = modo;
    }

    public String getModoProceso() {
        return modoProceso;
    }

    public void setModoProceso(String modoProceso) {
        this.modoProceso = modoProceso;
    }
    
    
}
