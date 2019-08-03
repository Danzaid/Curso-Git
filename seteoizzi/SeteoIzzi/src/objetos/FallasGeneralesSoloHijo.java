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
public class FallasGeneralesSoloHijo {
    private String tipoOrden;
    private String motivoOrden;
    private String Rowid;
    private String idpadre;
    private String categoria;
    private String motivofg;
    private String submotivo;
    private String solucion;

    public String getTipoOrden() {
        return tipoOrden;
    }

    public String getRowid() {
        return Rowid;
    }

    public void setRowid(String Rowid) {
        this.Rowid = Rowid;
    }

    public void setTipoOrden(String tipoOrden) {
        this.tipoOrden = tipoOrden;
    }

    public String getMotivoOrden() {
        return motivoOrden;
    }

    public void setMotivoOrden(String motivoOrden) {
        this.motivoOrden = motivoOrden;
    }

    public String getIdpadre() {
        return idpadre;
    }

    public void setIdpadre(String idpadre) {
        this.idpadre = idpadre;
    }

    public String getCategoria() {
        return categoria;
    }

    public void setCategoria(String categoria) {
        this.categoria = categoria;
    }

    public String getMotivofg() {
        return motivofg;
    }

    public void setMotivofg(String motivofg) {
        this.motivofg = motivofg;
    }

    public String getSubmotivo() {
        return submotivo;
    }

    public void setSubmotivo(String submotivo) {
        this.submotivo = submotivo;
    }

    public String getSolucion() {
        return solucion;
    }

    public void setSolucion(String solucion) {
        this.solucion = solucion;
    }
    
    
}
