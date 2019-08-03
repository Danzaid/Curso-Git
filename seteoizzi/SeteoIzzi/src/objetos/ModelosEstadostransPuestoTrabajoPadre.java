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
public class ModelosEstadostransPuestoTrabajoPadre {

    private String nombre;
    private String businessComponent;
    private String campo;
    private Date activacion;
    private Date vencimiento;
    private String comentarios;
    private String rowId;  

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public String getBusinessComponent() {
        return businessComponent;
    }

    public void setBusinessComponent(String businessComponent) {
        this.businessComponent = businessComponent;
    }

    public String getCampo() {
        return campo;
    }

    public void setCampo(String campo) {
        this.campo = campo;
    }

    public Date getActivacion() {
        return activacion;
    }

    public void setActivacion(Date activacion) {
        this.activacion = activacion;
    }

    public Date getVencimiento() {
        return vencimiento;
    }

    public void setVencimiento(Date vencimiento) {
        this.vencimiento = vencimiento;
    }

    public String getComentarios() {
        return comentarios;
    }

    public void setComentarios(String comentarios) {
        this.comentarios = comentarios;
    }

    public String getRowId() {
        return rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }

    

    
}
