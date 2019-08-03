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
public class DetalleDescAgregacion {
    private Integer cantidad;
    private String producto;
    private String comprar;
    private String recibir;
    private String tipoajuste;
    private Double importeajuste;
    private String srowid;
    private Integer secuencia;

    public Integer getSecuencia() {
        return secuencia;
    }
  
    public void setSecuencia(Integer secuencia) {
        this.secuencia = secuencia;
    }

    public Integer getCantidad() {
        return cantidad;
    }

    public void setCantidad(Integer cantidad) {
        this.cantidad = cantidad;
    }

    public String getProducto() {
        return producto;
    }

    public void setProducto(String producto) {
        this.producto = producto;
    }

    public String getComprar() {
        return comprar;
    }

    public void setComprar(String comprar) {
        this.comprar = comprar;
    }

    public String getRecibir() {
        return recibir;
    }

    public void setRecibir(String recibir) {
        this.recibir = recibir;
    }

    public String getTipoajuste() {
        return tipoajuste;
    }

    public void setTipoajuste(String tipoajuste) {
        this.tipoajuste = tipoajuste;
    }

    public Double getImporteajuste() {
        return importeajuste;
    }

    public void setImporteajuste(Double importeajuste) {
        this.importeajuste = importeajuste;
    }

    public String getSrowid() {
        return srowid;
    }

    public void setSrowid(String srowid) {
        this.srowid = srowid;
    }
}
