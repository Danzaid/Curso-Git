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
public class SeqDescAgregacionSoloHijo {

    private String secuencia;
    private String nombre;
    private String sigDescuentoSiUtiliza;
    private String sigDescuentoNoUtiliza;
    private String Rowid;
    private String IdPadre;
    private String nombreseqDescAgreg;

    public String getRowid() {
        return Rowid;
    }

    public void setRowid(String Rowid) {
        this.Rowid = Rowid;
    }

    public String getSecuencia() {
        return secuencia;
    }

    public void setSecuencia(String secuencia) {
        this.secuencia = secuencia;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public String getSigDescuentoSiUtiliza() {
        return sigDescuentoSiUtiliza;
    }

    public void setSigDescuentoSiUtiliza(String sigDescuentoSiUtiliza) {
        this.sigDescuentoSiUtiliza = sigDescuentoSiUtiliza;
    }

    public String getSigDescuentoNoUtiliza() {
        return sigDescuentoNoUtiliza;
    }

    public void setSigDescuentoNoUtiliza(String sigDescuentoNoUtiliza) {
        this.sigDescuentoNoUtiliza = sigDescuentoNoUtiliza;
    }

    public String getIdPadre() {
        return IdPadre;
    }

    public void setIdPadre(String IdPadre) {
        this.IdPadre = IdPadre;
    }

    public String getNombreseqDescAgreg() {
        return nombreseqDescAgreg;
    }

    public void setNombreseqDescAgreg(String nombreseqDescAgreg) {
        this.nombreseqDescAgreg = nombreseqDescAgreg;
    }

}
