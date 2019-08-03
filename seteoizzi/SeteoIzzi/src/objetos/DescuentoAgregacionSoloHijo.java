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
public class DescuentoAgregacionSoloHijo {
    
    
    private String secuencia;
    private String cantidad;
    private String nombreProducto;
    private String comprar;
    private String recibir;
    private String tipoAjuste;
    private String importeAjuste;
    private String rowid;
    private String idpadre;
    private String nombredescuento;

    public String getRowid() {
        return rowid;
    }

    public void setRowid(String rowid) {
        this.rowid = rowid;
    }
    
     
     public String getSecuencia() {
        return secuencia;
    }
    
    public void setSecuencia(String secuencia) {
        this.secuencia = secuencia;
    }
    
     public String getCantidad() {
        return cantidad;
    }
    
    public void setCantidad(String cantidad) {
        this.cantidad = cantidad;
    }
    
     public String getNombreProducto() {
        return nombreProducto;
    }
    
    public void setNombreProducto(String nombreProducto) {
        this.nombreProducto = nombreProducto;
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
     public String getTipoAjuste() {
        return tipoAjuste;
    }
    
    public void setTipoAjuste(String tipoAjuste) {
        this.tipoAjuste = tipoAjuste;
    }
    
    public String getImporteAjuste() {
        return importeAjuste;
    }
    
    public void setImporteAjuste(String importeAjuste) {
        this.importeAjuste = importeAjuste;
    }

    public String getIdpadre() {
        return idpadre;
    }

    public void setIdpadre(String idpadre) {
        this.idpadre = idpadre;
    }

    public String getNombredescuento() {
        return nombredescuento;
    }

    public void setNombredescuento(String nombredescuento) {
        this.nombredescuento = nombredescuento;
    }
    
}
