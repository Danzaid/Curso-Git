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
public class PlazoForzoso {
    private String producto;
    private String plazo;
    private String rowId;

    public String getRowId() {
        return rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }
    
    public String getProducto(){
            return producto;
    }
    public void setProducto (String producto){
            this.producto = producto;    
    }

    
    public String getPlazo(){
            return plazo;
    }
    public void setPlazo(String plazo){
            this.plazo = plazo;
    }
}
