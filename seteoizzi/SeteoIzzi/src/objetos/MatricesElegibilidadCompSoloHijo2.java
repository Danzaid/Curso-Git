/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package objetos;

import java.util.Date;

/**
 *
 * @author hector.pineda
 */
public class MatricesElegibilidadCompSoloHijo2 {
       private String idRegla;
       private String tipo;
       private Date fechaInicio;
       private Date fechaFinal;
       private String asuntoProducto;
       private String objetoProducto;
       private String Rowid;
       private String nombrematriz;

    public String getRowid() {
        return Rowid;
    }

    public void setRowid(String Rowid) {
        this.Rowid = Rowid;
    }

    public String getIdRegla() {
        return idRegla;
    }

    public void setIdRegla(String idRegla) {
        this.idRegla = idRegla;
    }

    public String getTipo() {
        return tipo;
    }

    public void setTipo(String tipo) {
        this.tipo = tipo;
    }

    public String getAsuntoProducto() {
        return asuntoProducto;
    }

    public void setAsuntoProducto(String asuntoProducto) {
        this.asuntoProducto = asuntoProducto;
    }

    public String getObjetoProducto() {
        return objetoProducto;
    }

    public void setObjetoProducto(String objetoProducto) {
        this.objetoProducto = objetoProducto;
    }

    public Date getFechaInicio() {
        return fechaInicio;
    }

    public void setFechaInicio(Date fechaInicio) {
        this.fechaInicio = fechaInicio;
    }

    public Date getFechaFinal() {
        return fechaFinal;
    }

    public void setFechaFinal(Date fechaFinal) {
        this.fechaFinal = fechaFinal;
    }

    public String getNombrematriz() {
        return nombrematriz;
    }

    public void setNombrematriz(String nombrematriz) {
        this.nombrematriz = nombrematriz;
    }
    
       
}
