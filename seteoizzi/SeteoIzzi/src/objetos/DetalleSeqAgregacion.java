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
public class DetalleSeqAgregacion {
    private Double secuencia;
    private String descAgrega;
    private Double sigDescSiUtil;
    private Double sigDescNoUtil;
    private String sRowId;

    public Double getSecuencia() {
        return secuencia;
    }

    public void setSecuencia(Double secuencia) {
        this.secuencia = secuencia;
    }

    public String getDescAgrega() {
        return descAgrega;
    }

    public void setDescAgrega(String descAgrega) {
        this.descAgrega = descAgrega;
    }

    public Double getSigDescSiUtil() {
        return sigDescSiUtil;
    }

    public void setSigDescSiUtil(Double sigDescSiUtil) {
        this.sigDescSiUtil = sigDescSiUtil;
    }

    public Double getSigDescNoUtil() {
        return sigDescNoUtil;
    }

    public void setSigDescNoUtil(Double sigDescNoUtil) {
        this.sigDescNoUtil = sigDescNoUtil;
    }

    public String getsRowId() {
        return sRowId;
    }

    public void setsRowId(String sRowId) {
        this.sRowId = sRowId;
    }
    
}
