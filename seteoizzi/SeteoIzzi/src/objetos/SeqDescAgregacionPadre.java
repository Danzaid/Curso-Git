/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package objetos;

import java.util.Date;

/**
 *
 * @author Felipe Gutierrez
 */
public class SeqDescAgregacionPadre {
    private String nombre;
    private Date vijenteDesde;
    private Date vijenteHasta;
    private String activo;
    private String comentario;
    private String Rowid;

    public String getRowid() {
        return Rowid;
    }

    public void setRowid(String Rowid) {
        this.Rowid = Rowid;
    }
    

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public Date getVijenteDesde() {
        return vijenteDesde;
    }

    public void setVijenteDesde(Date vijenteDesde) {
        this.vijenteDesde = vijenteDesde;
    }

    public Date getVijenteHasta() {
        return vijenteHasta;
    }

    public void setVijenteHasta(Date vijenteHasta) {
        this.vijenteHasta = vijenteHasta;
    }

    public String getActivo() {
        return activo;
    }

    public void setActivo(String activo) {
        this.activo = activo;
    }

    public String getComentario() {
        return comentario;
    }

    public void setComentario(String comentario) {
        this.comentario = comentario;
    }
    

    
    
}
