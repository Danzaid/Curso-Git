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
public class EventosTiempoEjecucion {//corregido
    private String nombre;
    private String secuencia;
    private String tipoObjeto;
    private String objectName;
    private String evento;
    private String subevento;
    private String exprecionCondicional;
    private String nombreJuegoAcciones;
    private String rowId;

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public String getSecuencia() {
        return secuencia;
    }

    public void setSecuencia(String secuencia) {
        this.secuencia = secuencia;
    }

    public String getTipoObjeto() {
        return tipoObjeto;
    }

    public void setTipoObjeto(String tipoObjeto) {
        this.tipoObjeto = tipoObjeto;
    }

    public String getObjectName() {
        return objectName;
    }

    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    public String getEvento() {
        return evento;
    }

    public void setEvento(String evento) {
        this.evento = evento;
    }

    public String getSubevento() {
        return subevento;
    }

    public void setSubevento(String subevento) {
        this.subevento = subevento;
    }

    public String getExprecionCondicional() {
        return exprecionCondicional;
    }

    public void setExprecionCondicional(String exprecionCondicional) {
        this.exprecionCondicional = exprecionCondicional;
    }

    public String getNombreJuegoAcciones() {
        return nombreJuegoAcciones;
    }

    public void setNombreJuegoAcciones(String nombreJuegoAcciones) {
        this.nombreJuegoAcciones = nombreJuegoAcciones;
    }

    public String getRowId() {
        return rowId;
    }

    public void setRowId(String rowId) {
        this.rowId = rowId;
    }
}
