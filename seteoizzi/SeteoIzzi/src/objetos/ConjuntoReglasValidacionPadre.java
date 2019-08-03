/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package objetos;

import java.util.Date;

/**
 *
 * @author alexis.alamilla
 */
public class ConjuntoReglasValidacionPadre { //corregido
    private String nombre;
    private String grupo;
    private String businessComponent;
    private String version;
    private String estado;
    private String businessObject;
    private String ver;
    private String expresionCondicional;
    private Date fechaInicio;
    private Date fechaFinal;
    private String agregacionErrores;
    private String descripcion;
    private String rowId;

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public String getGrupo() {
        return grupo;
    }

    public void setGrupo(String grupo) {
        this.grupo = grupo;
    }

    public String getBusinessComponent() {
        return businessComponent;
    }

    public void setBusinessComponent(String businessComponent) {
        this.businessComponent = businessComponent;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getEstado() {
        return estado;
    }

    public void setEstado(String estado) {
        this.estado = estado;
    }

    public String getBusinessObject() {
        return businessObject;
    }

    public void setBusinessObject(String businessObject) {
        this.businessObject = businessObject;
    }

    public String getVer() {
        return ver;
    }

    public void setVer(String ver) {
        this.ver = ver;
    }

    public String getExpresionCondicional() {
        return expresionCondicional;
    }

    public void setExpresionCondicional(String expresionCondicional) {
        this.expresionCondicional = expresionCondicional;
    }

    public String getAgregacionErrores() {
        return agregacionErrores;
    }

    public void setAgregacionErrores(String agregacionErrores) {
        this.agregacionErrores = agregacionErrores;
    }

    public String getDescripcion() {
        return descripcion;
    }

    public void setDescripcion(String descripcion) {
        this.descripcion = descripcion;
    }

    public String getRowId() {
        return rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }

    public Date getFechaInicio() {
        return fechaInicio;
    }

    public void setFechaInicio(Date fechaInicio) {
        this.fechaInicio = fechaInicio;
    }

    public Date getFechaFinal() {
        return fechaFinal;
    }

    public void setFechaFinal(Date fechaFinal) {
        this.fechaFinal = fechaFinal;
    }

    
    
}
