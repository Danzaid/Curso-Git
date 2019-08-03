/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package objetos;

/**
 *
 * @author Felipe Gutierrez
 */
public class ListaPreciosRPT {
    private String tipo;
    private String subTipo;
    private String digital;
    private String listaPrecios;
    private String codigoRPT;
    private String rptNombre;
    private String sRowId;

    public String getTipo() {
        return tipo;
    }

    public void setTipo(String tipo) {
        this.tipo = tipo;
    }

    public String getSubTipo() {
        return subTipo;
    }

    public void setSubTipo(String subTipo) {
        this.subTipo = subTipo;
    }

    public String getDigital() {
        return digital;
    }

    public void setDigital(String digital) {
        this.digital = digital;
    }

    public String getListaPrecios() {
        return listaPrecios;
    }

    public void setListaPrecios(String listaPrecios) {
        this.listaPrecios = listaPrecios;
    }

    public String getCodigoRPT() {
        return codigoRPT;
    }

    public void setCodigoRPT(String codigoRPT) {
        this.codigoRPT = codigoRPT;
    }

    public String getRptNombre() {
        return rptNombre;
    }

    public void setRptNombre(String rptNombre) {
        this.rptNombre = rptNombre;
    }

    public String getsRowId() {
        return sRowId;
    }

    public void setsRowId(String sRowId) {
        this.sRowId = sRowId;
    }
    
}
