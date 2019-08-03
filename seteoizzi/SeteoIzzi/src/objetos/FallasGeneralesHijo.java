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
public class FallasGeneralesHijo {
    private String tipoOrden;
    private String motivoOrden;
    private String Rowid;

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
    
}
