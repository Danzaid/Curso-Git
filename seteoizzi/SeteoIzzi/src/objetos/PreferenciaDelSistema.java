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
public class PreferenciaDelSistema {
    private String prefDelSistema;
    private String rowId;

    public String getRowId() {
        return rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }
    
    public String getPrefDelSistemas (){
        return prefDelSistema;
    }
    public void setPrefDelSistema (String prefDelSistema){
        this.prefDelSistema = prefDelSistema;
    }
    
}
