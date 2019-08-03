/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package objetos;

/**
 *
 * @author hector.pineda
 */
public class GrupoPolizasPadre {
    private String nombreGrupoPoliza;
    private String descripcion;
    private String Rowid;

    public String getRowid() {
        return Rowid;
    }

    public void setRowid(String Rowid) {
        this.Rowid = Rowid;
    }

    public String getNombreGrupoPoliza() {
        return nombreGrupoPoliza;
    }

    public void setNombreGrupoPoliza(String nombreGrupoPoliza) {
        this.nombreGrupoPoliza = nombreGrupoPoliza;
    }

    public String getDescripcion() {
        return descripcion;
    }

    public void setDescripcion(String descripcion) {
        this.descripcion = descripcion;
    }
    
    
    
}
