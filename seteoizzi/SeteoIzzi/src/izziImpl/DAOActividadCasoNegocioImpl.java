/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package izziImpl;

import com.siebel.data.SiebelBusComp;
import com.siebel.data.SiebelBusObject;
import com.siebel.data.SiebelDataBean;
import com.siebel.data.SiebelException;
import dao.ConexionDB;
import dao.ConexionSiebel;
import dao.Reportes;
import interfaces.DAOActividadCasoNegocio;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import objetos.ActividadCasoNegocioHijo;
import objetos.ActividadCasoNegocioPadre;
import objetos.ActividadCasoNegocioSoloHijo;

/**
 *
 * @author hector.pineda
 */
public class DAOActividadCasoNegocioImpl extends ConexionDB implements DAOActividadCasoNegocio {

    @Override
    public void inserta(List<ActividadCasoNegocioPadre> actividadCasoNegocio, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);

        Boolean conteopadre = actividadCasoNegocio.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

            for (ActividadCasoNegocioPadre sAdminC : actividadCasoNegocio) {
                if ("SI".equals(Conectar)) {
                    conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                    Conectar = "NO";
                }

                try {

                    String MsgError = "";

                    SiebelBusObject BO = m_dataBean.getBusObject("Activity Template");
                    SiebelBusComp BC = BO.getBusComp("Activity Template");
                    SiebelBusComp BCCHILD = BO.getBusComp("Template Activity");
                    SiebelBusComp oBCPick = new SiebelBusComp();

                    BC.activateField("TT Name");
                    BC.activateField("CV SR Nivel Tipificacion 1");
                    BC.activateField("CV SR Nivel Tipificacion 2");
                    BC.activateField("CV SR Nivel Tipificacion 3");
                    BC.activateField("TT SR Nivel Tipificacion 4");
                    BC.activateField("Public");
                    BC.activateField("TT Expired");
                    BC.activateField("TT Type");
                    BC.activateField("Type");
                    BC.clearToQuery();
                    BC.setSearchExpr("[TT Name] ='" + sAdminC.getNombre() + "'");
                    BC.executeQuery(true);
                    boolean reg = BC.firstRecord();
                    if (reg) {
                        if (sAdminC.getCategoria() != null) {
                            if (!BC.getFieldValue("CV SR Nivel Tipificacion 1").equals(sAdminC.getCategoria())) {
                                oBCPick = BC.getPicklistBusComp("CV SR Nivel Tipificacion 1");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Value] ='" + sAdminC.getCategoria() + "' ");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getMotivo() != null) {
                            if (!BC.getFieldValue("CV SR Nivel Tipificacion 2").equals(sAdminC.getMotivo())) {
                                oBCPick = BC.getPicklistBusComp("CV SR Nivel Tipificacion 2");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Parent] ='" + sAdminC.getCategoria() + "' AND [Value]='" + sAdminC.getMotivo() + "'");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getSubMotivo() != null) {
                            if (!BC.getFieldValue("CV SR Nivel Tipificacion 3").equals(sAdminC.getSubMotivo())) {
                                oBCPick = BC.getPicklistBusComp("CV SR Nivel Tipificacion 3");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Parent] ='" + sAdminC.getMotivo() + "' AND [Value]='" + sAdminC.getSubMotivo() + "'");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getSolución() != null) {
                            if (!BC.getFieldValue("TT SR Nivel Tipificacion 4").equals(sAdminC.getSolución())) {
                                oBCPick = BC.getPicklistBusComp("TT SR Nivel Tipificacion 4");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Parent] ='" + sAdminC.getSubMotivo() + "' AND [Value]='" + sAdminC.getSolución() + "'");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getActivacionAutomatica() != null) {
                            if (!BC.getFieldValue("Public").equals(sAdminC.getActivacionAutomatica())) {
                                BC.setFieldValue("Public", sAdminC.getActivacionAutomatica());
                            }
                        }

                        if (sAdminC.getVencido() != null) {
                            if (!BC.getFieldValue("TT Expired").equals(sAdminC.getVencido())) {
                                BC.setFieldValue("TT Expired", sAdminC.getVencido());
                            }
                        }

                        if (sAdminC.getTipoPlantilla() != null) {
                            if (!BC.getFieldValue("TT Type").equals(sAdminC.getTipoPlantilla())) {
                                oBCPick = BC.getPicklistBusComp("TT Type");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Value] ='" + sAdminC.getTipoPlantilla() + "' ");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getTipo() != null) {
                            if (!BC.getFieldValue("Type").equals(sAdminC.getTipo())) {
                                oBCPick = BC.getPicklistBusComp("Type");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Value] ='" + sAdminC.getTipo() + "' ");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Plantilla de Actividad CN:  " + sAdminC.getNombre());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Plantilla de Actividad CN";

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

//                        String RowId1 = BC.getFieldValue("Id");
//                        this.cargaBC(BCCHILD, oBCPick, RowId1, sAdminC.getRowId(),usuario,version,ambienteInser,ambienteExtra);

                        BCCHILD.release();
                        BC.release();
                        BO.release();

                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getNombre() != null) {
                            BC.setFieldValue("TT Name", sAdminC.getNombre());
                        }

                        if (sAdminC.getCategoria() != null) {
                            oBCPick = BC.getPicklistBusComp("CV SR Nivel Tipificacion 1");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value] ='" + sAdminC.getCategoria() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getMotivo() != null) {
                            oBCPick = BC.getPicklistBusComp("CV SR Nivel Tipificacion 2");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Parent] ='" + sAdminC.getCategoria() + "' AND [Value]='" + sAdminC.getMotivo() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getSubMotivo() != null) {
                            oBCPick = BC.getPicklistBusComp("CV SR Nivel Tipificacion 3");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Parent] ='" + sAdminC.getMotivo() + "' AND [Value]='" + sAdminC.getSubMotivo() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getSolución() != null) {
                            oBCPick = BC.getPicklistBusComp("TT SR Nivel Tipificacion 4");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Parent] ='" + sAdminC.getSubMotivo() + "' AND [Value]='" + sAdminC.getSolución() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getActivacionAutomatica() != null) {
                            BC.setFieldValue("Public", sAdminC.getActivacionAutomatica());
                        }

                        if (sAdminC.getVencido() != null) {
                            BC.setFieldValue("TT Expired", sAdminC.getVencido());
                        }

                        if (sAdminC.getTipoPlantilla() != null) {
                            oBCPick = BC.getPicklistBusComp("TT Type");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value] ='" + sAdminC.getTipoPlantilla() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getTipo() != null) {
                            oBCPick = BC.getPicklistBusComp("Type");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value] ='" + sAdminC.getTipo() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        BC.writeRecord();

                        System.out.println("Se crea registro de Plantilla de Actividad CN: " + sAdminC.getNombre());
                        String mensaje = ("Se crea registro de Plantilla de Actividad CN: " + sAdminC.getNombre());
                        String MsgSalida = "Creado Plantilla de Actividad CN";

                        this.CargaBitacoraSalidaCreado(sAdminC.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

//                        String RowID = BC.getFieldValue("Id");
//                        this.cargaBC(BCCHILD, oBCPick, RowID, sAdminC.getRowId(),usuario,version,ambienteInser,ambienteExtra);

                        BCCHILD.release();
                        BC.release();
                        BO.release();

                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en creacion de Plantilla de Actividad CN:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Plantilla de Actividad CN";
                    String FlagCarga = "E";
                    String MsgError = "Error en creacion de Plantilla de Actividad CNo:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " ");

                    this.CargaBitacoraSalidaError(sAdminC.getRowId(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);
                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgError);

                } finally {

                    Contador++;
//                System.out.println("Contador =  " + Contador);

                    if (Contador == Maxima) {
                        conSiebel.CloseSiebel(m_dataBean);
                        Contador = 0;
                        Conectar = "SI";
//                    System.out.println("Se inicia contador a =  " + Contador);
                    }
                }
            }
        }
        try {                                                                       // BUSCA REGISTROS NUEVOS O ACTUALIZADOS, DESDE EL HIJO - Plantilla de Actividad CN
            List<ActividadCasoNegocioSoloHijo> hijo = new LinkedList();
            hijo = this.consultaSoloHijo(fechaIni, fechaTer,usuario,version,ambienteInser,ambienteExtra);

            Boolean conteosolohijo = hijo.isEmpty(); // VALIDA SI LA LISTA ESTA VACIA
            if (!conteosolohijo) {

                ConexionSiebel conSiebel = new ConexionSiebel();
                SiebelDataBean m_dataBean = new SiebelDataBean();
                conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

                String NombrePadre = "";
                String IdPadre = null;

                for (ActividadCasoNegocioSoloHijo sAdminC : hijo) {  // SI LA LISTA NO ESTA VACIA, COMIENZA EJECUCION
                    if ("SI".equals(Conectar)) {
                        conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                        Conectar = "NO";
                    }
                    try {
                        SiebelBusObject BO = m_dataBean.getBusObject("Activity Template");
                        SiebelBusComp BC = BO.getBusComp("Activity Template");
                        SiebelBusComp BCCHILD = BO.getBusComp("Template Activity");
                        SiebelBusComp oBCPick = new SiebelBusComp();

                        BC.activateField("TT Name");
                        BC.clearToQuery();
                        BC.setSearchExpr("[TT Name] ='" + sAdminC.getNombreacn() + "'");  // BUSCA PADRE PARA OBTENER ROW_ID DEL AMBIENTE DESTINO
                        BC.executeQuery(true);
                        boolean reg = BC.firstRecord();
                        if (reg) {
                            IdPadre = BC.getFieldValue("Id");  // OBTIENE ROW_ID DEL AMBIENTE DESTINO

                        } else {
                            System.out.println("No se encontro registro de Lista de Poliza con nombre: " + sAdminC.getNombreacn() + " , en el ambiente a insertar, por esta razon no puede validarse si existen HIJOS a modificar o crear. ");
                            String MsgSalida = "No se encontro registro de Lista de Poliza con nombre: " + sAdminC.getNombreacn() + " , en el ambiente a insertar, por esta razon no puede validarse si existen HIJOS a modificar o crear. ";
                            String FlagCarga = "E";
                            String MsgError = "No se encontro registro de Lista de Poliza con nombre: " + sAdminC.getNombreacn() + " , en el ambiente a insertar, por esta razon no puede validarse si existen HIJOS a modificar o crear. ";

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgError);
                            this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                            BCCHILD.release();
                            BC.release();
                            BO.release();
                        }

                        if (IdPadre != null) {                          // SI SE ENCONTRO ROW_ID DE PADRE, COMIENZA BUSQUEDA DE HIJOS 1 - CONDICIONES
                            BCCHILD.activateField("Type");
                            BCCHILD.activateField("Descripción");
                            BCCHILD.activateField("Duración");
                            BCCHILD.activateField("Área de conocimiento");
                            BCCHILD.activateField("Última actividad");
                            BCCHILD.activateField("Linea");
                            BCCHILD.activateField("Unidades");
                            BCCHILD.activateField("Estado de la orden");
                            BCCHILD.activateField("Subestado de la orden");
                            BCCHILD.activateField("Soluciona CN");
                            BCCHILD.activateField("Enviar Remedy");
                            BCCHILD.activateField("Código Remedy");
                            BCCHILD.activateField("Formulario Dinámico");
                            BCCHILD.activateField("Nombre de formulario");
                            BCCHILD.activateField("No soluciono");
                            BCCHILD.activateField("Template Id");
                            BCCHILD.clearToQuery();
                            BCCHILD.setSearchExpr("[Type] ='" + sAdminC.getTipo() + "' AND [Template Id] ='" + IdPadre + "'");
                            BCCHILD.executeQuery(true);
                            boolean regchild = BCCHILD.firstRecord();
                            if (regchild) {

                                if (sAdminC.getDescripcion() != null) {
                                    if (!BCCHILD.getFieldValue("Description").equals(sAdminC.getDescripcion())) {
                                        BCCHILD.setFieldValue("Description", sAdminC.getDescripcion());
                                    }
                                }

                                if (sAdminC.getDuracion() != null) {
                                    if (!BCCHILD.getFieldValue("Duration Minutes").equals(sAdminC.getDuracion())) {
                                        oBCPick = BCCHILD.getPicklistBusComp("Duration Minutes");
                                        oBCPick.clearToQuery();
                                        oBCPick.setSearchExpr("[Value] ='" + sAdminC.getDuracion() + "' ");
                                        oBCPick.executeQuery(true);
                                        if (oBCPick.firstRecord()) {
                                            oBCPick.pick();
                                        }
                                        oBCPick.release();
                                    }
                                }

                                if (sAdminC.getUltimaActividad() != null) {
                                    if (!BCCHILD.getFieldValue("TT Is Last Activity").equals(sAdminC.getUltimaActividad())) {
                                        BCCHILD.setFieldValue("TT Is Last Activity", sAdminC.getUltimaActividad());
                                    }
                                }

                                if (sAdminC.getUnidades() != null) {
                                    if (!BCCHILD.getFieldValue("Units").equals(sAdminC.getUnidades())) {
                                        oBCPick = BCCHILD.getPicklistBusComp("Units");
                                        oBCPick.clearToQuery();
                                        oBCPick.setSearchExpr("[Value] ='" + sAdminC.getUnidades() + "' ");
                                        oBCPick.executeQuery(true);
                                        if (oBCPick.firstRecord()) {
                                            oBCPick.pick();
                                        }
                                        oBCPick.release();
                                    }
                                }

                                if (sAdminC.getEstadoOrden() != null) {
                                    if (!BCCHILD.getFieldValue("TT Estado Orden").equals(sAdminC.getEstadoOrden())) {
                                        oBCPick = BCCHILD.getPicklistBusComp("TT Estado Orden");
                                        oBCPick.clearToQuery();
                                        oBCPick.setSearchExpr("[Value] ='" + sAdminC.getEstadoOrden() + "' ");
                                        oBCPick.executeQuery(true);
                                        if (oBCPick.firstRecord()) {
                                            oBCPick.pick();
                                        }
                                        oBCPick.release();
                                    }
                                }

                                if (sAdminC.getSubestadoOrden() != null) {
                                    if (!BCCHILD.getFieldValue("TT Subestado Orden").equals(sAdminC.getSubestadoOrden())) {
                                        oBCPick = BCCHILD.getPicklistBusComp("TT Subestado Orden");
                                        oBCPick.clearToQuery();
                                        oBCPick.setSearchExpr("[Parent] ='" + sAdminC.getEstadoOrden() + "' AND [Value]='" + sAdminC.getSubestadoOrden() + "'");
                                        oBCPick.executeQuery(true);
                                        if (oBCPick.firstRecord()) {
                                            oBCPick.pick();
                                        }
                                        oBCPick.release();
                                    }
                                }

                                if (sAdminC.getSolucionaCN() != null) {
                                    if (!BCCHILD.getFieldValue("TT SR Closed Flag").equals(sAdminC.getSolucionaCN())) {
                                        BCCHILD.setFieldValue("TT SR Closed Flag", sAdminC.getSolucionaCN());
                                    }
                                }

                                if (sAdminC.getEnviarRemedy() != null) {
                                    if (!BCCHILD.getFieldValue("TT Envia Remedy").equals(sAdminC.getEnviarRemedy())) {
                                        BCCHILD.setFieldValue("TT Envia Remedy", sAdminC.getEnviarRemedy());
                                    }
                                }

                                if (sAdminC.getCodigoRemedy() != null) {
                                    if (!BCCHILD.getFieldValue("TT Codigo Remedy").equals(sAdminC.getCodigoRemedy())) {
                                        oBCPick = BCCHILD.getPicklistBusComp("TT Codigo Remedy");
                                        oBCPick.clearToQuery();
                                        oBCPick.setSearchExpr("[Remedy Id] ='" + sAdminC.getCodigoRemedy() + "' ");
                                        oBCPick.executeQuery(true);
                                        if (oBCPick.firstRecord()) {
                                            oBCPick.pick();
                                        }
                                        oBCPick.release();
                                    }
                                }

                                if (sAdminC.getFormularioDinamico() != null) {
                                    if (!BCCHILD.getFieldValue("TT Dynamic Form Flag").equals(sAdminC.getFormularioDinamico())) {
                                        BCCHILD.setFieldValue("TT Dynamic Form Flag", sAdminC.getFormularioDinamico());
                                    }
                                }

                                if (sAdminC.getNombreFormulario() != null) {
                                    if (!BCCHILD.getFieldValue("TT Activity Form Name").equals(sAdminC.getNombreFormulario())) {
                                        BCCHILD.setFieldValue("TT Activity Form Name", sAdminC.getNombreFormulario());
                                    }
                                }

                                if (sAdminC.getNoSoluciono() != null) {
                                    if (!BCCHILD.getFieldValue("TT No Soluciono").equals(sAdminC.getNoSoluciono())) {
                                        BCCHILD.setFieldValue("TT No Soluciono", sAdminC.getNoSoluciono());
                                    }
                                }

                                BCCHILD.writeRecord();

                                System.out.println("Se valida existencia y/o actualizacion de Plantilla de Actividad CN:  " + sAdminC.getDescripcion() + " : " + sAdminC.getTipo());
                                String MsgSalida = "Se valida existencia y/o actualizacion de Plantilla de Actividad CN";

                                this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(MsgSalida);

                                BCCHILD.release();
                                BC.release();
                                BO.release();

                            } else {
                                BCCHILD.newRecord(0);

                                if (sAdminC.getDescripcion() != null) {
                                    BCCHILD.setFieldValue("Description", sAdminC.getDescripcion());
                                }

                                if (sAdminC.getDuracion() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("Duration Minutes");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Value] ='" + sAdminC.getDuracion() + "' ");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getUltimaActividad() != null) {
                                    BCCHILD.setFieldValue("TT Is Last Activity", sAdminC.getUltimaActividad());
                                }

                                if (sAdminC.getUnidades() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("Units");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Value] ='" + sAdminC.getUnidades() + "' ");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getEstadoOrden() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("TT Estado Orden");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Value] ='" + sAdminC.getEstadoOrden() + "' ");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getSubestadoOrden() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("TT Subestado Orden");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Parent] ='" + sAdminC.getEstadoOrden() + "' AND [Value]='" + sAdminC.getSubestadoOrden() + "'");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getSolucionaCN() != null) {
                                    BCCHILD.setFieldValue("TT SR Closed Flag", sAdminC.getSolucionaCN());
                                }

                                if (sAdminC.getEnviarRemedy() != null) {
                                    BCCHILD.setFieldValue("TT Envia Remedy", sAdminC.getEnviarRemedy());
                                }

                                if (sAdminC.getCodigoRemedy() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("TT Codigo Remedy");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Remedy Id] ='" + sAdminC.getCodigoRemedy() + "' ");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getFormularioDinamico() != null) {
                                    BCCHILD.setFieldValue("TT Dynamic Form Flag", sAdminC.getFormularioDinamico());
                                }

                                if (sAdminC.getNombreFormulario() != null) {
                                    BCCHILD.setFieldValue("TT Activity Form Name", sAdminC.getNombreFormulario());
                                }

                                if (sAdminC.getNoSoluciono() != null) {
                                    BCCHILD.setFieldValue("TT No Soluciono", sAdminC.getNoSoluciono());
                                }

                                BCCHILD.setFieldValue("Template Id", IdPadre);
                                BCCHILD.writeRecord();

                                System.out.println("Se creo Plantilla de Actividad CN:  " + sAdminC.getDescripcion() + " : " + sAdminC.getTipo());
                                String mensaje = ("Se creo Plantilla de Actividad CN:  " + sAdminC.getDescripcion() + " : " + sAdminC.getTipo());
                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(mensaje);

                                String MsgSalida = "Creado Plantilla de Actividad CN";
                                this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                BCCHILD.release();
                                BC.release();
                                BO.release();

                            }
                        }
                        IdPadre = null;

                    } catch (SiebelException e) {
                        String error = e.getErrorMessage();
                        System.out.println("Error en creacion de Plantilla de Actividad CN:  " + sAdminC.getDescripcion() + " : " + sAdminC.getTipo() + "     , con el mensaje:   " + error.replace("'", " "));
                        String MsgSalida = "Error al Crear Plantilla de Actividad CN";
                        String FlagCarga = "E";
                        String MsgError = "Error en creacion de Plantilla de Actividad CN:  " + sAdminC.getDescripcion() + " : " + sAdminC.getTipo() + "     , con el mensaje:   " + error.replace("'", " ");

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgError);

                        this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                    } finally {

                        Contador++;
//                System.out.println("Contador =  " + Contador);

                        if (Contador == Maxima) {
                            conSiebel.CloseSiebel(m_dataBean);
                            Contador = 0;
                            Conectar = "SI";
//                    System.out.println("Se inicia contador a =  " + Contador);
                        }
                    }
                }
            }
        } catch (SiebelException e) {
            String error = e.getErrorMessage();
            System.out.println("Error en  validacion de Plantilla de Actividad CN, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en  validacion de Plantilla de Actividad CN, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);

        }
    }

    @Override
    public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick, String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws SiebelException {
        try {
            List<ActividadCasoNegocioHijo> hijo = new LinkedList();
            hijo = this.consultaHijo(IdPadre,usuario,version,ambienteExtra,ambienteInser);
            for (ActividadCasoNegocioHijo sAdminC : hijo) {
                try {
                    BC.activateField("Type");
                    BC.activateField("Descripción");
                    BC.activateField("Duración");
                    BC.activateField("Área de conocimiento");
                    BC.activateField("Última actividad");
                    BC.activateField("Linea");
                    BC.activateField("Unidades");
                    BC.activateField("Estado de la orden");
                    BC.activateField("Subestado de la orden");
                    BC.activateField("Soluciona CN");
                    BC.activateField("Enviar Remedy");
                    BC.activateField("Código Remedy");
                    BC.activateField("Formulario Dinámico");
                    BC.activateField("Nombre de formulario");
                    BC.activateField("No soluciono");
                    BC.activateField("Template Id");
                    BC.clearToQuery();
                    BC.setSearchExpr("[Type] ='" + sAdminC.getTipo() + "' ");
                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {

                        if (sAdminC.getDescripcion() != null) {
                            if (!BC.getFieldValue("Description").equals(sAdminC.getDescripcion())) {
                                BC.setFieldValue("Description", sAdminC.getDescripcion());
                            }
                        }

                        if (sAdminC.getDuracion() != null) {
                            if (!BC.getFieldValue("Duration Minutes").equals(sAdminC.getDuracion())) {
                                oBCPick = BC.getPicklistBusComp("Duration Minutes");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Value] ='" + sAdminC.getDuracion() + "' ");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getUltimaActividad() != null) {
                            if (!BC.getFieldValue("TT Is Last Activity").equals(sAdminC.getUltimaActividad())) {
                                BC.setFieldValue("TT Is Last Activity", sAdminC.getUltimaActividad());
                            }
                        }

                        if (sAdminC.getUnidades() != null) {
                            if (!BC.getFieldValue("Units").equals(sAdminC.getUnidades())) {
                                oBCPick = BC.getPicklistBusComp("Units");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Value] ='" + sAdminC.getUnidades() + "' ");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getEstadoOrden() != null) {
                            if (!BC.getFieldValue("TT Estado Orden").equals(sAdminC.getEstadoOrden())) {
                                oBCPick = BC.getPicklistBusComp("TT Estado Orden");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Value] ='" + sAdminC.getEstadoOrden() + "' ");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getSubestadoOrden() != null) {
                            if (!BC.getFieldValue("TT Subestado Orden").equals(sAdminC.getSubestadoOrden())) {
                                oBCPick = BC.getPicklistBusComp("TT Subestado Orden");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Parent] ='" + sAdminC.getEstadoOrden() + "' AND [Value]='" + sAdminC.getSubestadoOrden() + "'");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getSolucionaCN() != null) {
                            if (!BC.getFieldValue("TT SR Closed Flag").equals(sAdminC.getSolucionaCN())) {
                                BC.setFieldValue("TT SR Closed Flag", sAdminC.getSolucionaCN());
                            }
                        }

                        if (sAdminC.getEnviarRemedy() != null) {
                            if (!BC.getFieldValue("TT Envia Remedy").equals(sAdminC.getEnviarRemedy())) {
                                BC.setFieldValue("TT Envia Remedy", sAdminC.getEnviarRemedy());
                            }
                        }

                        if (sAdminC.getCodigoRemedy() != null) {
                            if (!BC.getFieldValue("TT Codigo Remedy").equals(sAdminC.getCodigoRemedy())) {
                                oBCPick = BC.getPicklistBusComp("TT Codigo Remedy");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Remedy Id] ='" + sAdminC.getCodigoRemedy() + "' ");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getFormularioDinamico() != null) {
                            if (!BC.getFieldValue("TT Dynamic Form Flag").equals(sAdminC.getFormularioDinamico())) {
                                BC.setFieldValue("TT Dynamic Form Flag", sAdminC.getFormularioDinamico());
                            }
                        }

                        if (sAdminC.getNombreFormulario() != null) {
                            if (!BC.getFieldValue("TT Activity Form Name").equals(sAdminC.getNombreFormulario())) {
                                BC.setFieldValue("TT Activity Form Name", sAdminC.getNombreFormulario());
                            }
                        }

                        if (sAdminC.getNoSoluciono() != null) {
                            if (!BC.getFieldValue("TT No Soluciono").equals(sAdminC.getNoSoluciono())) {
                                BC.setFieldValue("TT No Soluciono", sAdminC.getNoSoluciono());
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Detalle de Plantilla de Actividad CN:  " + sAdminC.getDescripcion() + " : " + sAdminC.getTipo());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Detalle de Plantilla de Actividad CN";

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getDescripcion() != null) {
                            BC.setFieldValue("Description", sAdminC.getDescripcion());
                        }

                        if (sAdminC.getDuracion() != null) {
                            oBCPick = BC.getPicklistBusComp("Duration Minutes");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value] ='" + sAdminC.getDuracion() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getUltimaActividad() != null) {
                            BC.setFieldValue("TT Is Last Activity", sAdminC.getUltimaActividad());
                        }

                        if (sAdminC.getUnidades() != null) {
                            oBCPick = BC.getPicklistBusComp("Units");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value] ='" + sAdminC.getUnidades() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getEstadoOrden() != null) {
                            oBCPick = BC.getPicklistBusComp("TT Estado Orden");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value] ='" + sAdminC.getEstadoOrden() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getSubestadoOrden() != null) {
                            oBCPick = BC.getPicklistBusComp("TT Subestado Orden");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Parent] ='" + sAdminC.getEstadoOrden() + "' AND [Value]='" + sAdminC.getSubestadoOrden() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getSolucionaCN() != null) {
                            BC.setFieldValue("TT SR Closed Flag", sAdminC.getSolucionaCN());
                        }

                        if (sAdminC.getEnviarRemedy() != null) {
                            BC.setFieldValue("TT Envia Remedy", sAdminC.getEnviarRemedy());
                        }

                        if (sAdminC.getCodigoRemedy() != null) {
                            oBCPick = BC.getPicklistBusComp("TT Codigo Remedy");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Remedy Id] ='" + sAdminC.getCodigoRemedy() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getFormularioDinamico() != null) {
                            BC.setFieldValue("TT Dynamic Form Flag", sAdminC.getFormularioDinamico());
                        }

                        if (sAdminC.getNombreFormulario() != null) {
                            BC.setFieldValue("TT Activity Form Name", sAdminC.getNombreFormulario());
                        }

                        if (sAdminC.getNoSoluciono() != null) {
                            BC.setFieldValue("TT No Soluciono", sAdminC.getNoSoluciono());
                        }

                        BC.setFieldValue("Template Id", RowID);
                        BC.writeRecord();

                        System.out.println("Se creo Detalle de Plantilla de Actividad CN:  " + sAdminC.getDescripcion() + " : " + sAdminC.getTipo());
                        String mensaje = ("Se creo Detalle de Plantilla de Actividad CN:  " + sAdminC.getDescripcion() + " : " + sAdminC.getTipo());
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

                        String MsgSalida = "Creado Detalle de Plantilla de Actividad CN";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();

                    String MsgSalida = "Error en creacion de Detalle de Plantilla de Actividad CN";
                    String FlagCarga = "E";
                    String MsgError = "Error en creacion de Detalle de Plantilla de Actividad CN.   " + sAdminC.getDescripcion() + "     , con el mensaje:   " + error.replace("'", " ");
                    System.out.println("Error en creacion de Detalle de Plantilla de Actividad CN.   " + sAdminC.getDescripcion() + "     , con el mensaje:   " + error.replace("'", " "));

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgError);

                    this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);
                }
            }
        } catch (SiebelException e) {
            String error = e.getErrorMessage();
            System.out.println("Error en cargaBC, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en cargaBC, con el error:  " + error.replace("'", " ");

        } catch (Exception ex) {
            Logger.getLogger(DAOActividadCasoNegocioImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void getHora() {

        try {
            Calendar now = Calendar.getInstance();

            System.out.println("Current full date time is : " + (now.get(Calendar.MONTH) + 1) + "-"
                    + now.get(Calendar.DATE) + "-" + now.get(Calendar.YEAR) + " "
                    + now.get(Calendar.HOUR_OF_DAY) + ":" + now.get(Calendar.MINUTE) + ":"
                    + now.get(Calendar.SECOND) + "." + now.get(Calendar.MILLISECOND));
        } catch (Exception e) {
            throw e;
        }

        try {
            Calendar now = Calendar.getInstance();

            String hora = ("" + (now.get(Calendar.MONTH) + 1) + "-"
                    + now.get(Calendar.DATE) + "-" + now.get(Calendar.YEAR) + " "
                    + now.get(Calendar.HOUR_OF_DAY) + ":" + now.get(Calendar.MINUTE) + ":"
                    + now.get(Calendar.SECOND) + "." + now.get(Calendar.MILLISECOND));
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(hora);
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public List<ActividadCasoNegocioPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.X_TEMP_NAME \"Nombre\", A.TYPE_CD \"Tipo\", A.X_SR_NIVEL_1 \"Categoría\", A.X_SR_NIVEL_2 \"Motivo\", A.X_SR_NIVEL_3 \"Sub Motivo\", A.X_TT_SR_NIVEL_4 \"Solución\", A.PUBLIC_FLG \"Activación automática\", \n"
                + "A.X_EXPIRED_FLG \"Vencido\", A.X_TT_TYPE \"Tipo de plantilla\"\n"
                + "FROM SIEBEL.S_TMPL_PLANITEM A, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<ActividadCasoNegocioPadre> actividadCasoNegocio = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Padre a procesar.");

            while (rs.next()) {
                ActividadCasoNegocioPadre lista = new ActividadCasoNegocioPadre();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setCategoria(rs.getString("Categoría"));
                lista.setMotivo(rs.getString("Motivo"));
                lista.setSubMotivo(rs.getString("Sub Motivo"));
                lista.setSolución(rs.getString("Solución"));
                lista.setActivacionAutomatica(rs.getString("Activación automática"));
                lista.setVencido(rs.getString("Vencido"));
                lista.setTipoPlantilla(rs.getString("Tipo de plantilla"));
                lista.setTipo(rs.getString("Tipo"));
                actividadCasoNegocio.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre"), rs.getString("Categoría"), rs.getString("Motivo"), "SE OBTIENEN DATOS", "PLANTILLAS DE ACTIVIDAD CN",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = actividadCasoNegocio.size();
            Boolean conteo = actividadCasoNegocio.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Padres - Actividad Casos de Negocio o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Padres - Actividad Casos de Negocio o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Padres - Actividad Casos de Negocio, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Padres - Actividad Casos de Negocio, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return actividadCasoNegocio;
    }

    @Override
    public List<ActividadCasoNegocioHijo> consultaHijo(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.TMPL_PLANITEM_ID \"Id Padre\", B.TODO_CD \"Tipo\", B.SUBTYPE_CD \"Categoria\", B.OBJECTIVE_CD \"Descripción\", B.APPT_DURATION_MIN \"Duración\", B.OBJECTIVE_CD \"Área de conocimiento\", \n"
                + "C.ATTRIB_08 \"Última actividad\", D.LINE_NUM \"Linea\", B.LEAD_TM_UOM_CD \"Unidades\", B.X_TT_ESTATUS_ORDEN \"Estado de la orden\", B.X_TT_SUBESTATUS_ORDEN \"Subestado de la orden\", C.ATTRIB_10 \"Soluciona CN\",\n"
                + " C.ATTRIB_11 \"Enviar Remedy\", B.X_TT_RMD_CODIGO \"Código Remedy\", B.APPT_CALL_FLG \"Formulario Dinámico\", B.X_TT_NAME_FORM \"Nombre de formulario\", B.X_NO_SOLUCIONO \"No soluciono\"\n"
                + "FROM SIEBEL.S_EVT_ACT B, SIEBEL.S_EVT_ACT_X C, SIEBEL.S_EVT_TASK D, SIEBEL.S_USER H\n"
                + "WHERE B.ROW_ID = C.PAR_ROW_ID (+) AND B.ROW_ID = D.PAR_TSK_ID(+) AND B.LAST_UPD_BY = H.ROW_ID\n"
                + "AND B.TMPL_PLANITEM_ID = '" + IdPadre + "' AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')"
                + "ORDER BY B.CREATED ASC";
        List<ActividadCasoNegocioHijo> actividadCasoNegocio = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                ActividadCasoNegocioHijo lista = new ActividadCasoNegocioHijo();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setTipo(rs.getString("Tipo"));
                lista.setDescripcion(rs.getString("Descripción"));
                lista.setDuracion(rs.getString("Duración"));
                lista.setAreaConocimiento(rs.getString("Área de conocimiento"));
                lista.setUltimaActividad(rs.getString("Última actividad"));
                lista.setLinea(rs.getString("Linea"));
                lista.setUnidades(rs.getString("Unidades"));
                lista.setEstadoOrden(rs.getString("Estado de la orden"));
                lista.setSubestadoOrden(rs.getString("Subestado de la orden"));
                lista.setSolucionaCN(rs.getString("Soluciona CN"));
                lista.setEnviarRemedy(rs.getString("Enviar Remedy"));
                lista.setCodigoRemedy(rs.getString("Código Remedy"));
                lista.setFormularioDinamico(rs.getString("Formulario Dinámico"));
                lista.setNombreFormulario(rs.getString("Nombre de formulario"));
                lista.setCategoria(rs.getString("Categoria"));
                lista.setNoSoluciono(rs.getString("No soluciono"));
                actividadCasoNegocio.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Tipo"), rs.getString("Descripción"), rs.getString("Duración"), "SE OBTIENEN DATOS HIJO", "PLANTILLAS DE ACTIVIDAD CN - DETALLE",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = actividadCasoNegocio.size();
            Boolean conteo = actividadCasoNegocio.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Detalle de Plantilla de Actividad CN o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Detalle de Plantilla de Actividad CN o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Detalle de Plantilla de Actividad CN, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Detalle de Plantilla de Actividad CN, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo, con el error:  " + ex);
            throw ex;
        }
        return actividadCasoNegocio;
    }

    @Override
    public List<ActividadCasoNegocioSoloHijo> consultaSoloHijo(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.TMPL_PLANITEM_ID \"Id Padre\", B.TODO_CD \"Tipo\", B.SUBTYPE_CD \"Categoria\", B.OBJECTIVE_CD \"Descripción\", B.APPT_DURATION_MIN \"Duración\", B.OBJECTIVE_CD \"Área de conocimiento\",\n"
                + "C.ATTRIB_08 \"Última actividad\", D.LINE_NUM \"Linea\", B.LEAD_TM_UOM_CD \"Unidades\", B.X_TT_ESTATUS_ORDEN \"Estado de la orden\", B.X_TT_SUBESTATUS_ORDEN \"Subestado de la orden\", C.ATTRIB_10 \"Soluciona CN\",\n"
                + "C.ATTRIB_11 \"Enviar Remedy\", B.X_TT_RMD_CODIGO \"Código Remedy\", B.APPT_CALL_FLG \"Formulario Dinámico\", B.X_TT_NAME_FORM \"Nombre de formulario\", B.X_NO_SOLUCIONO \"No soluciono\", A.X_TEMP_NAME \"Nombre ACN\"\n"
                + "FROM SIEBEL.S_EVT_ACT B, SIEBEL.S_EVT_ACT_X C, SIEBEL.S_EVT_TASK D, SIEBEL.S_TMPL_PLANITEM A,SIEBEL.S_USER H\n"
                + "WHERE B.ROW_ID = C.PAR_ROW_ID (+) AND B.ROW_ID = D.PAR_TSK_ID AND B.TMPL_PLANITEM_ID (+)= A.ROW_ID AND B.LAST_UPD_BY = H.ROW_ID\n"
                + "AND  H.LOGIN = '" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA E WHERE E.ROW_ID = B.ROW_ID AND E.USUARIO ='" + usuario + "' AND E.VERSION ='" + version + "' AND E.EXTRAER ='" + ambienteExtra + "' AND E.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY B.LAST_UPD ASC";
        List<ActividadCasoNegocioSoloHijo> actividadCasoNegociosolohijo = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                ActividadCasoNegocioSoloHijo lista = new ActividadCasoNegocioSoloHijo();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setTipo(rs.getString("Tipo"));
                lista.setDescripcion(rs.getString("Descripción"));
                lista.setDuracion(rs.getString("Duración"));
                lista.setAreaConocimiento(rs.getString("Área de conocimiento"));
                lista.setUltimaActividad(rs.getString("Última actividad"));
                lista.setLinea(rs.getString("Linea"));
                lista.setUnidades(rs.getString("Unidades"));
                lista.setEstadoOrden(rs.getString("Estado de la orden"));
                lista.setSubestadoOrden(rs.getString("Subestado de la orden"));
                lista.setSolucionaCN(rs.getString("Soluciona CN"));
                lista.setEnviarRemedy(rs.getString("Enviar Remedy"));
                lista.setCodigoRemedy(rs.getString("Código Remedy"));
                lista.setFormularioDinamico(rs.getString("Formulario Dinámico"));
                lista.setNombreFormulario(rs.getString("Nombre de formulario"));
                lista.setCategoria(rs.getString("Categoria"));
                lista.setNoSoluciono(rs.getString("No soluciono"));
                lista.setIdpadre(rs.getString("Id Padre"));
                lista.setNombreacn(rs.getString("Nombre ACN"));
                actividadCasoNegociosolohijo.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Tipo"), rs.getString("Descripción"), rs.getString("Duración"), "SE OBTIENEN DATOS HIJO", "PLANTILLAS DE ACTIVIDAD CN - DETALLE",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = actividadCasoNegociosolohijo.size();
            Boolean conteo = actividadCasoNegociosolohijo.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Detalle de Plantilla de Actividad CN o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Detalle de Plantilla de Actividad CN o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Detalle de Plantilla de Actividad CN, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Detalle de Plantilla de Actividad CN, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaSoloHijo, con el error:  " + ex);
            throw ex;
        }
        return actividadCasoNegociosolohijo;
    }

    private void CargaBitacoraEntrada(String Row_Id, String Val1, String Val2, String Val3, String Seguimiento, String Objeto,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String TipoObjeto = Val1 + " : " + Val2 + " : " + Val3;

        String searchRecordSQL1 = "SELECT ROW_ID FROM SIEBEL.CT_BITACORA WHERE ROW_ID = '" + Row_Id + "' AND USUARIO = '" + usuario + "' AND VERSION = '" + version + "' AND EXTRAER = '" + ambienteExtra + "' AND INSERTAR = '" + ambienteInser + "'";

        try (PreparedStatement ps = conexion.prepareStatement(searchRecordSQL1); ResultSet rs1 = ps.executeQuery()) {
            if (rs1.next()) {
                // sin accion
            } else {
                String setRecordSQL2 = "INSERT INTO SIEBEL.CT_BITACORA  (ROW_ID, TIPO_CATALOGO, TIPO_OBJETO, FLAG_CARGA, SEGUIMIENTO, MENSAJE_ERROR,USUARIO, VERSION, EXTRAER, INSERTAR)\n"
                        + "VALUES('" + Row_Id + "','" + Objeto + "','" + TipoObjeto + "','N','" + Seguimiento + "','','" + usuario + "','" + version + "','" + ambienteExtra + "','" + ambienteInser + "')";

                PreparedStatement ps1 = conexion.prepareStatement(setRecordSQL2);
                ps1.executeUpdate();
                ps1.close();
            }
            rs1.close();
            ps.close();

        }
    }

    private void CargaBitacoraSalidaCreado(String rowId, String MsgSalida, String FlagCarga, String MsgError,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String readRecordSQL4 = "UPDATE SIEBEL.CT_BITACORA SET FLAG_CARGA = '" + FlagCarga + "', SEGUIMIENTO = '" + MsgSalida + "', MENSAJE_ERROR = '" + MsgError + "'\n"
                + "WHERE ROW_ID = '" + rowId + "' AND USUARIO = '" + usuario + "' AND VERSION = '" + version + "' AND EXTRAER = '" + ambienteExtra + "' AND INSERTAR = '" + ambienteInser + "' ";

        PreparedStatement ps3 = conexion.prepareStatement(readRecordSQL4);
        ps3.executeUpdate();
        ps3.close();
    }

    private void CargaBitacoraSalidaValidado(String rowId, String MsgSalida, String FlagCarga, String MsgError,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String searchRecordSQL3 = "SELECT ROW_ID FROM SIEBEL.CT_BITACORA \n"
                + "WHERE FLAG_CARGA= 'Y' AND SEGUIMIENTO LIKE '%Creado%' AND ROW_ID = '" + rowId + "' AND USUARIO = '" + usuario + "' AND VERSION = '" + version + "' AND EXTRAER = '" + ambienteExtra + "' AND INSERTAR = '" + ambienteInser + "'";

        PreparedStatement ps3 = conexion.prepareStatement(searchRecordSQL3);
        if (ps3.executeQuery().next()) {
            // sin accion
        } else {
            String readRecordSQL5 = "UPDATE SIEBEL.CT_BITACORA SET FLAG_CARGA = '" + FlagCarga + "', SEGUIMIENTO = '" + MsgSalida + "', MENSAJE_ERROR = '" + MsgError + "'\n"
                    + "WHERE ROW_ID = '" + rowId + "'";

            PreparedStatement ps4 = conexion.prepareStatement(readRecordSQL5);
            ps4.executeUpdate();
            ps4.close();
        }
        ps3.close();
    }

    private void CargaBitacoraSalidaError(String rowId, String MsgSalida, String FlagCarga, String MsgError,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String readRecordSQL6 = "UPDATE SIEBEL.CT_BITACORA SET FLAG_CARGA = '" + FlagCarga + "', SEGUIMIENTO = '" + MsgSalida + "', MENSAJE_ERROR = '" + MsgError + "'\n"
                + "WHERE ROW_ID = '" + rowId + "' AND USUARIO = '" + usuario + "' AND VERSION = '" + version + "' AND EXTRAER = '" + ambienteExtra + "' AND INSERTAR = '" + ambienteInser + "'";

        PreparedStatement ps5 = conexion.prepareStatement(readRecordSQL6);
        ps5.executeUpdate();
        ps5.close();
    }

}
