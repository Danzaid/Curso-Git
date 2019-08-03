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
import interfaces.DAOControladorCasosNegocios;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import objetos.ControladorCasosNegocioHijo;
import objetos.ControladorCasosNegocioPadre;
import objetos.ControladorCasosNegocioSoloHijo;

/**
 *
 * @author hector.pineda
 */
public class DAOControladorCasosNegocioImpl extends ConexionDB implements DAOControladorCasosNegocios {

    @Override
    public void inserta(List<ControladorCasosNegocioPadre> controladorCasosNegocio, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);
        String ErrorPick = null;

        Boolean conteopadre = controladorCasosNegocio.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

            for (ControladorCasosNegocioPadre sAdminC : controladorCasosNegocio) {
                if ("SI".equals(Conectar)) {
                    conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                    Conectar = "NO";
                }

                try {

                    String MsgError = "";

                    SiebelBusObject BO = m_dataBean.getBusObject("TT Controlador Casos Negocio");
                    SiebelBusComp BC = BO.getBusComp("TT Controlador Casos Negocio");
                    SiebelBusComp BCCHILD = BO.getBusComp("TT Template Formulario Dinamico");
                    SiebelBusComp oBCPick = new SiebelBusComp();

                    BC.activateField("TT CN Nivel Tipificacion 1");
                    BC.activateField("TT CN Nivel Tipificacion 2");
                    BC.activateField("TT CN Nivel Tipificacion 3");
                    BC.activateField("TT CN Nivel Tipificacion 4");
                    BC.activateField("TT CN Nom Formulario");
                    BC.activateField("TT CN Nom Plantilla");
                    BC.activateField("TT CN Nuevo Posicion");
                    BC.activateField("TT CN Unico");
                    BC.activateField("TT Email Flag");
                    BC.activateField("TT Order Type");
                    BC.activateField("TT Modicar Comentario");
                    BC.activateField("TT Accion HIT");
                    BC.activateField("TT Flg Equipo Telefonia");
                    BC.activateField("TT Interfaz Refresh");
                    BC.activateField("TT Mirada Type");
                    BC.activateField("TT Motivo Cliente");
                    BC.activateField("TT Required Fields Name");
                    BC.activateField("TT Remedy Monitor");
                    BC.activateField("TT CN Admon Mail");
                    BC.activateField("TT Asociado FG Flag");
                    BC.activateField("TT Submotivo");
                    BC.activateField("TT Swat Flag");
                    BC.activateField("TT Formulario Dinamico Flag");
                    BC.activateField("TT CN Cliente Recurrente Flag");
                    BC.activateField("TT Troubleshooting");
                    BC.activateField("TT Field Req Flag");
                    BC.activateField("TT Opera Sucursal");
                    BC.activateField("TT Flag Visita Tecnica");
                    BC.activateField("TT RPT Equipos");
                    BC.activateField("TT Flag Cierre de Actividad");
                    BC.activateField("TT Primary RPT Excluded");
                    BC.activateField("TT Sync Navigator");
                    BC.clearToQuery();
                    BC.setSearchSpec("TT CN Nivel Tipificacion 1", sAdminC.getCategoria());
                    BC.setSearchSpec("TT CN Nivel Tipificacion 2", sAdminC.getMotivo());
                    BC.setSearchSpec("TT CN Nivel Tipificacion 3", sAdminC.getSubMotivo());
                    BC.setSearchSpec("TT CN Nivel Tipificacion 4", sAdminC.getSolucion());
                    BC.executeQuery(true);
                    boolean reg = BC.firstRecord();
                    if (reg) {
                        // SE ACTUALIZAN CAMPOS DE PADRE

                        if (sAdminC.getNombreFormulario() != null) {
                            if (!BC.getFieldValue("TT CN Nom Formulario").equals(sAdminC.getNombreFormulario())) {
                                BC.setFieldValue("TT CN Nom Formulario", sAdminC.getNombreFormulario());
                            }
                        }
                        if (sAdminC.getPlantillaActividad() != null) {
                            if (!BC.getFieldValue("TT CN Nom Plantilla").equals(sAdminC.getPlantillaActividad())) {
                                BC.setFieldValue("TT CN Nom Plantilla", sAdminC.getPlantillaActividad());
                            }
                        }
                        if (sAdminC.getCnUnico() != null) {
                            if (!BC.getFieldValue("TT CN Unico").equals(sAdminC.getCnUnico())) {
                                BC.setFieldValue("TT CN Unico", sAdminC.getCnUnico());
                            }
                        }

                        if (sAdminC.getEmailSMS() != null) {
                            if (!BC.getFieldValue("TT Email Flag").equals(sAdminC.getEmailSMS())) {
                                BC.setFieldValue("TT Email Flag", sAdminC.getEmailSMS());
                            }
                        }
                        if (sAdminC.getOrdenServicio() != null) {
                            if (!BC.getFieldValue("TT Order Type").equals(sAdminC.getOrdenServicio())) {
                                oBCPick = BC.getPicklistBusComp("TT Order Type");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Order Type] ='" + sAdminC.getOrdenServicio() + "'");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getModificarComentario() != null) {
                            if (!BC.getFieldValue("TT Modicar Comentario").equals(sAdminC.getModificarComentario())) {
                                BC.setFieldValue("TT Modicar Comentario", sAdminC.getModificarComentario());
                            }
                        }
                        if (sAdminC.getIndicadorAccionHIT() != null) {
                            if (!BC.getFieldValue("TT Accion HIT").equals(sAdminC.getIndicadorAccionHIT())) {
                                BC.setFieldValue("TT Accion HIT", sAdminC.getIndicadorAccionHIT());
                            }
                        }
                        if (sAdminC.getCopiarEquiposTelefonia() != null) {
                            if (!BC.getFieldValue("TT Flg Equipo Telefonia").equals(sAdminC.getCopiarEquiposTelefonia())) {
                                BC.setFieldValue("TT Flg Equipo Telefonia", sAdminC.getCopiarEquiposTelefonia());
                            }
                        }
                        if (sAdminC.getProcesoInterfazRefresh() != null) {
                            if (!BC.getFieldValue("TT Interfaz Refresh").equals(sAdminC.getProcesoInterfazRefresh())) {
                                BC.setFieldValue("TT Interfaz Refresh", sAdminC.getProcesoInterfazRefresh());
                            }
                        }
                        if (sAdminC.getTroubleMirada() != null) {
                            if (!BC.getFieldValue("TT Mirada Type").equals(sAdminC.getTroubleMirada())) {
                                BC.setFieldValue("TT Mirada Type", sAdminC.getTroubleMirada());
                            }
                        }
                        if (sAdminC.getMotivoCTE() != null) {
                            if (!BC.getFieldValue("TT Motivo Cliente").equals(sAdminC.getMotivoCTE())) {
                                BC.setFieldValue("TT Motivo Cliente", sAdminC.getMotivoCTE());
                            }
                        }
                        if (sAdminC.getRemedyMonitor() != null) {
                            if (!BC.getFieldValue("TT Remedy Monitor").equals(sAdminC.getRemedyMonitor())) {
                                BC.setFieldValue("TT Remedy Monitor", sAdminC.getRemedyMonitor());
                            }
                        }
                        if (sAdminC.getCnAdministrativoMail() != null) {
                            if (!BC.getFieldValue("TT CN Admon Mail").equals(sAdminC.getCnAdministrativoMail())) {
                                BC.setFieldValue("TT CN Admon Mail", sAdminC.getCnAdministrativoMail());
                            }
                        }
                        if (sAdminC.getAsociarFallaGeneral() != null) {
                            if (!BC.getFieldValue("TT Asociado FG Flag").equals(sAdminC.getAsociarFallaGeneral())) {
                                BC.setFieldValue("TT Asociado FG Flag", sAdminC.getAsociarFallaGeneral());
                            }
                        }
                        if (sAdminC.getSubmotivoFG() != null) {
                            if (!BC.getFieldValue("TT Submotivo").equals(sAdminC.getSubmotivoFG())) {
                                oBCPick = BC.getPicklistBusComp("TT Submotivo");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Value] ='" + sAdminC.getSubmotivoFG() + "'");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getAplicaSwatTeam() != null) {
                            if (!BC.getFieldValue("TT Swat Flag").equals(sAdminC.getAplicaSwatTeam())) {
                                BC.setFieldValue("TT Swat Flag", sAdminC.getAplicaSwatTeam());
                            }
                        }
                        if (sAdminC.getFormularioDinamico() != null) {
                            if (!BC.getFieldValue("TT Formulario Dinamico Flag").equals(sAdminC.getFormularioDinamico())) {
                                BC.setFieldValue("TT Formulario Dinamico Flag", sAdminC.getFormularioDinamico());
                            }
                        }
                        if (sAdminC.getCnRecurrente() != null) {
                            if (!BC.getFieldValue("TT CN Cliente Recurrente Flag").equals(sAdminC.getCnRecurrente())) {
                                BC.setFieldValue("TT CN Cliente Recurrente Flag", sAdminC.getCnRecurrente());
                            }
                        }
                        if (sAdminC.getTroubleShooting() != null) {
                            if (!BC.getFieldValue("TT Troubleshooting").equals(sAdminC.getTroubleShooting())) {
                                BC.setFieldValue("TT Troubleshooting", sAdminC.getTroubleShooting());
                            }
                        }
                        if (sAdminC.getCamposRequeridos2() != null) {
                            if (!BC.getFieldValue("TT Field Req Flag").equals(sAdminC.getCamposRequeridos2())) {
                                BC.setFieldValue("TT Field Req Flag", sAdminC.getCamposRequeridos2());
                            }
                        }
                        if (sAdminC.getOperaSucursal() != null) {
                            if (!BC.getFieldValue("TT Opera Sucursal").equals(sAdminC.getOperaSucursal())) {
                                BC.setFieldValue("TT Opera Sucursal", sAdminC.getOperaSucursal());
                            }
                        }
                        if (sAdminC.getVistaTecnica() != null) {
                            if (!BC.getFieldValue("TT Flag Visita Tecnica").equals(sAdminC.getVistaTecnica())) {
                                BC.setFieldValue("TT Flag Visita Tecnica", sAdminC.getVistaTecnica());
                            }
                        }
                        if (sAdminC.getCierreActividad() != null) {
                            if (!BC.getFieldValue("TT Flag Cierre de Actividad").equals(sAdminC.getCierreActividad())) {
                                BC.setFieldValue("TT Flag Cierre de Actividad", sAdminC.getCierreActividad());
                            }
                        }
                        if (sAdminC.getRestringirCreacionRTP() != null) {
                            if (!BC.getFieldValue("TT Primary RPT Excluded").equals(sAdminC.getRestringirCreacionRTP())) {
                                BC.setFieldValue("TT Primary RPT Excluded", sAdminC.getRestringirCreacionRTP());
                            }
                        }
                        if (sAdminC.getSyncNavigator() != null) {
                            if (!BC.getFieldValue("TT Sync Navigator").equals(sAdminC.getSyncNavigator())) {
                                BC.setFieldValue("TT Sync Navigator", sAdminC.getSyncNavigator());
                            }
                        }
                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion Controlador Casos de Negocio:  " + sAdminC.getCategoria() + " : " + sAdminC.getMotivo() + " : " + sAdminC.getSubMotivo());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Controlador Casos de Negocio";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                        // SE BUSCAN HIJOS
//                        String RowId1 = BC.getFieldValue("Id");
//                        this.cargaBC(BCCHILD, oBCPick, RowId1, sAdminC.getRowId());

                        BCCHILD.release();
                        BC.release();
                        BO.release();
                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getCategoria() != null) {
                            oBCPick = BC.getPicklistBusComp("TT CN Nivel Tipificacion 1");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value]='" + sAdminC.getCategoria() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            } else {
                                ErrorPick = "No se encontro LOV de correspondencia en el ambiente destino para el campo Categoria";
                            }

                            oBCPick.release();
                        } else {
                            ErrorPick = "El valor para el campo Categoria, viene nulo desde el ambiente base y es requerido para crear el registro";
                        }

                        if (sAdminC.getMotivo() != null) {
                            oBCPick = BC.getPicklistBusComp("TT CN Nivel Tipificacion 2");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Parent]='" + sAdminC.getCategoria() + "' AND [Value]= '" + sAdminC.getMotivo() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            } else {
                                ErrorPick = "No se encontro LOV de correspondencia en el ambiente destino para el campo Motivo";
                            }
                            oBCPick.release();
                        } else {
                            ErrorPick = "El valor para el campo Motivo, viene nulo desde el ambiente base y es requerido para crear el registro";
                        }

                        if (sAdminC.getSubMotivo() != null) {
                            oBCPick = BC.getPicklistBusComp("TT CN Nivel Tipificacion 3");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Parent]='" + sAdminC.getMotivo() + "' AND [Value]= '" + sAdminC.getSubMotivo() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            } else {
                                ErrorPick = "No se encontro LOV de correspondencia en el ambiente destino para el campo Sub Motivo";
                            }
                            oBCPick.release();
                        } else {
                            ErrorPick = "El valor para el campo Sub Motivo, viene nulo desde el ambiente base y es requerido para crear el registro";
                        }

                        if (sAdminC.getSolucion() != null) {
                            oBCPick = BC.getPicklistBusComp("TT CN Nivel Tipificacion 4");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Parent]='" + sAdminC.getSubMotivo() + "' AND [Value]= '" + sAdminC.getSolucion() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            } else {
                                ErrorPick = "No se encontro LOV de correspondencia en el ambiente destino para el campo Solucion";
                            }
                            oBCPick.release();
                        } else {
                            ErrorPick = "El valor para el campo Solucion, viene nulo desde el ambiente base y es requerido para crear el registro";
                        }

                        if (sAdminC.getNombreFormulario() != null) {
                            BC.setFieldValue("TT CN Nom Formulario", sAdminC.getNombreFormulario());
                        }

                        if (sAdminC.getPlantillaActividad() != null) {
                            BC.setFieldValue("TT CN Nom Plantilla", sAdminC.getPlantillaActividad());
                        }

                        if (sAdminC.getCnUnico() != null) {
                            BC.setFieldValue("TT CN Unico", sAdminC.getCnUnico());
                        }

                        if (sAdminC.getEmailSMS() != null) {
                            BC.setFieldValue("TT Email Flag", sAdminC.getEmailSMS());
                        }

                        if (sAdminC.getOrdenServicio() != null) {
                            oBCPick = BC.getPicklistBusComp("TT Order Type");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Order Type] ='" + sAdminC.getOrdenServicio() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getModificarComentario() != null) {
                            BC.setFieldValue("TT Modicar Comentario", sAdminC.getModificarComentario());
                        }

                        if (sAdminC.getIndicadorAccionHIT() != null) {
                            BC.setFieldValue("TT Accion HIT", sAdminC.getIndicadorAccionHIT());
                        }

                        if (sAdminC.getCopiarEquiposTelefonia() != null) {
                            BC.setFieldValue("TT Flg Equipo Telefonia", sAdminC.getCopiarEquiposTelefonia());
                        }

                        if (sAdminC.getProcesoInterfazRefresh() != null) {
                            BC.setFieldValue("TT Interfaz Refresh", sAdminC.getProcesoInterfazRefresh());
                        }

                        if (sAdminC.getTroubleMirada() != null) {
                            BC.setFieldValue("TT Mirada Type", sAdminC.getTroubleMirada());
                        }

                        if (sAdminC.getMotivoCTE() != null) {
                            BC.setFieldValue("TT Motivo Cliente", sAdminC.getMotivoCTE());
                        }

                        if (sAdminC.getRemedyMonitor() != null) {
                            BC.setFieldValue("TT Remedy Monitor", sAdminC.getRemedyMonitor());
                        }

                        if (sAdminC.getCnAdministrativoMail() != null) {
                            BC.setFieldValue("TT CN Admon Mail", sAdminC.getCnAdministrativoMail());
                        }

                        if (sAdminC.getAsociarFallaGeneral() != null) {
                            BC.setFieldValue("TT Asociado FG Flag", sAdminC.getAsociarFallaGeneral());
                        }

                        if (sAdminC.getSubmotivoFG() != null) {
                            oBCPick = BC.getPicklistBusComp("TT Submotivo");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value] ='" + sAdminC.getSubmotivoFG() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getAplicaSwatTeam() != null) {
                            BC.setFieldValue("TT Swat Flag", sAdminC.getAplicaSwatTeam());
                        }

                        if (sAdminC.getFormularioDinamico() != null) {
                            BC.setFieldValue("TT Formulario Dinamico Flag", sAdminC.getFormularioDinamico());
                        }

                        if (sAdminC.getCnRecurrente() != null) {
                            BC.setFieldValue("TT CN Cliente Recurrente Flag", sAdminC.getCnRecurrente());
                        }

                        if (sAdminC.getTroubleShooting() != null) {
                            BC.setFieldValue("TT Troubleshooting", sAdminC.getTroubleShooting());
                        }

                        if (sAdminC.getCamposRequeridos2() != null) {
                            BC.setFieldValue("TT Field Req Flag", sAdminC.getCamposRequeridos2());
                        }

                        if (sAdminC.getOperaSucursal() != null) {
                            BC.setFieldValue("TT Opera Sucursal", sAdminC.getOperaSucursal());
                        }

                        if (sAdminC.getVistaTecnica() != null) {
                            BC.setFieldValue("TT Flag Visita Tecnica", sAdminC.getVistaTecnica());
                        }

                        if (sAdminC.getCierreActividad() != null) {
                            BC.setFieldValue("TT Flag Cierre de Actividad", sAdminC.getCierreActividad());
                        }

                        if (sAdminC.getRestringirCreacionRTP() != null) {
                            BC.setFieldValue("TT Primary RPT Excluded", sAdminC.getRestringirCreacionRTP());
                        }

                        if (sAdminC.getSyncNavigator() != null) {
                            BC.setFieldValue("TT Sync Navigator", sAdminC.getSyncNavigator());
                        }

                        BC.writeRecord();

                        System.out.println("Se crea registro de Controlador Casos de Negocio: " + sAdminC.getCategoria() + " : " + sAdminC.getMotivo() + " : " + sAdminC.getSubMotivo());
                        String mensaje = ("Se crea registro de Controlador Casos de Negocio: " + sAdminC.getCategoria() + " : " + sAdminC.getMotivo() + " : " + sAdminC.getSubMotivo());

                        String MsgSalida = "Creado Controlador Casos de Negocio";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

                        // SE BUSCAN LOS HIJOS
//                        String RowID = BC.getFieldValue("Id");
//                        this.cargaBC(BCCHILD, oBCPick, RowID, sAdminC.getRowId());

                        BCCHILD.release();
                        BC.release();
                        BO.release();

                    }
                } catch (SiebelException e) {

                    String error;
                    String MsgSalida;
                    String FlagCarga = "E";
                    String MsgError;
                    if (ErrorPick == null) {
                        error = e.getErrorMessage();
                        System.out.println("Error en creacion Controlador Casos de Negocio:  " + sAdminC.getCategoria() + " : " + sAdminC.getMotivo() + " : " + sAdminC.getSubMotivo() + "     , con el mensaje:   " + error.replace("'", " "));
                        MsgSalida = "Error al Crear Controlador Casos de Negocio";
                        MsgError = "Error en creacion Controlador Casos de Negocio:  " + sAdminC.getCategoria() + " : " + sAdminC.getMotivo() + " : " + sAdminC.getSubMotivo() + "     , con el mensaje:   " + error.replace("'", " ");
                    } else {
                        System.out.println("Error en creacion Controlador Casos de Negocio:  " + ErrorPick);
                        MsgSalida = "Error al Crear Controlador Casos de Negocio";
                        MsgError = "Error en creacion Controlador Casos de Negocio:  " + ErrorPick;
                    }
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

        try {                                                                       // BUSCA REGISTROS NUEVOS O ACTUALIZADOS, DESDE EL HIJO 
            List<ControladorCasosNegocioSoloHijo> hijo = new LinkedList();
            hijo = this.consultaSoloHijo(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra);

            Boolean conteosolohijo = hijo.isEmpty(); // VALIDA SI LA LISTA ESTA VACIA
            if (!conteosolohijo) {

                ConexionSiebel conSiebel = new ConexionSiebel();
                SiebelDataBean m_dataBean = new SiebelDataBean();
                conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

                String NombrePadre = "";
                String IdPadre = null;

                for (ControladorCasosNegocioSoloHijo sAdminC : hijo) {  // SI LA LISTA NO ESTA VACIA, COMIENZA EJECUCION
                    if ("SI".equals(Conectar)) {
                        conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                        Conectar = "NO";
                    }
                    try {
                        SiebelBusObject BO = m_dataBean.getBusObject("TT Controlador Casos Negocio");
                        SiebelBusComp BC = BO.getBusComp("TT Controlador Casos Negocio");
                        SiebelBusComp BCCHILD = BO.getBusComp("TT Template Formulario Dinamico");
                        SiebelBusComp oBCPick = new SiebelBusComp();

                        BC.activateField("TT CN Nivel Tipificacion 1");
                        BC.activateField("TT CN Nivel Tipificacion 2");
                        BC.activateField("TT CN Nivel Tipificacion 3");
                        BC.activateField("TT CN Nivel Tipificacion 4");
                        BC.clearToQuery();
                        BC.setSearchSpec("TT CN Nivel Tipificacion 1", sAdminC.getCategoriacontrolador());
                        BC.setSearchSpec("TT CN Nivel Tipificacion 2", sAdminC.getMotivocontrolador());
                        BC.setSearchSpec("TT CN Nivel Tipificacion 3", sAdminC.getSubmotivocontrolador());
                        BC.setSearchSpec("TT CN Nivel Tipificacion 4", sAdminC.getSolucioncontrolador());  // BUSCA PADRE PARA OBTENER ROW_ID DEL AMBIENTE DESTINO
                        BC.executeQuery(true);
                        boolean reg = BC.firstRecord();
                        if (reg) {
                            IdPadre = BC.getFieldValue("Id");  // OBTIENE ROW_ID DEL AMBIENTE DESTINO
                            
                            // SI SE ENCONTRO ROW_ID DE PADRE, COMIENZA BUSQUEDA DE HIJOS 1 - CONDICIONES
                            BCCHILD.activateField("TT Etiqueta");
                            BCCHILD.activateField("TT Tipo dato");
                            BCCHILD.activateField("TT Requerido Flag");
                            BCCHILD.activateField("TT CN Id"); // Id de Padre
                            BCCHILD.clearToQuery();
                            BCCHILD.setSearchExpr("[TT Etiqueta] ='" + sAdminC.getEtiquetaCampo() + "' AND [TT CN Id] ='" + IdPadre + "'");
                            BCCHILD.executeQuery(true);
                            boolean regchild = BCCHILD.firstRecord();
                            if (regchild) {
                                if (sAdminC.getTipoDato() != null) {
                                    if (!BCCHILD.getFieldValue("TT Tipo dato").equals(sAdminC.getTipoDato())) {
                                        oBCPick = BCCHILD.getPicklistBusComp("TT Tipo dato");
                                        oBCPick.clearToQuery();
                                        oBCPick.setSearchExpr("[Value] ='" + sAdminC.getTipoDato() + "'");
                                        oBCPick.executeQuery(true);
                                        if (oBCPick.firstRecord()) {
                                            oBCPick.pick();
                                        }
                                        oBCPick.release();
                                    }
                                }

                                if (sAdminC.getRequerido() != null) {
                                    if (!BCCHILD.getFieldValue("TT Requerido Flag").equals(sAdminC.getRequerido())) {
                                    }
                                }
                                BCCHILD.writeRecord();

                                System.out.println("Se valida existencia y/o actualizacion de Formulario de Controlador CN :  " + sAdminC.getEtiquetaCampo());
                                String MsgSalida = "Se valida existencia y/o actualizacion de Formulario de Controlador CN";
                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(MsgSalida);

                                this.CargaBitacoraSalidaValidado(sAdminC.getRowId(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                BCCHILD.release();
                                BC.release();
                                BO.release();
                            } else {
                                BCCHILD.newRecord(0);

                                if (sAdminC.getEtiquetaCampo() != null) {
                                    BCCHILD.setFieldValue("TT Etiqueta", sAdminC.getEtiquetaCampo());
                                }

                                if (sAdminC.getTipoDato() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("TT Tipo dato");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Value] ='" + sAdminC.getTipoDato() + "'");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getRequerido() != null) {
                                    BCCHILD.setFieldValue("TT Requerido Flag", sAdminC.getRequerido());
                                }

                                BCCHILD.setFieldValue("TT CN Id", IdPadre);

                                BCCHILD.writeRecord();

                                System.out.println("Se creo Formulario:  " + sAdminC.getEtiquetaCampo());
                                String mensaje = ("Se creo Formulario:  " + sAdminC.getEtiquetaCampo());
                                String MsgSalida = "Creado Formulario de Controlador CN";

                                this.CargaBitacoraSalidaCreado(sAdminC.getRowId(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(mensaje);

                                BCCHILD.release();
                                BC.release();
                                BO.release();
                            }

                        } else {
                            System.out.println("No se encontro registro de Controlador de Casos de Negocio en el ambiente a insertar, por esta razon no puede validarse si existen FORMULARIOS a modificar o crear. ");
                            String MsgSalida = "No se encontro registro de Controlador de Casos de Negocio en el ambiente a insertar, por esta razon no puede validarse si existen FORMULARIOS a modificar o crear. ";
                            String FlagCarga = "E";
                            String MsgError = "No se encontro registro de Controlador de Casos de Negocio en el ambiente a insertar, por esta razon no puede validarse si existen FORMULARIOS a modificar o crear. ";

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgError);
                            this.CargaBitacoraSalidaError(sAdminC.getRowId(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                            BCCHILD.release();
                            BC.release();
                            BO.release();

                        }
                        
                    } catch (SiebelException e) {
                        String error = e.getErrorMessage();
                        System.out.println("Error en creacion de Formulario de Controlador CN:  " + sAdminC.getEtiquetaCampo() + "     , con el mensaje:   " + error.replace("'", " "));
                        String MsgSalida = "Error al Crear Formulario de Controlador CNn";
                        String FlagCarga = "E";
                        String MsgError = "Error en creacion de Formulario de Controlador CN:  " + sAdminC.getEtiquetaCampo() + "     , con el mensaje:   " + error.replace("'", " ");

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgError);

                        this.CargaBitacoraSalidaError(sAdminC.getRowId(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

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
            System.out.println("Error en  validacion de Formulario de Controlador CN, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en  validacion de Formulario de Controlador CN, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);

        }

    }

    @Override
    public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick, String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws SiebelException {
        try {
            List<ControladorCasosNegocioHijo> hijo = new LinkedList();
            hijo = this.consultaHijo(IdPadre,usuario,version,ambienteInser,ambienteExtra);
            for (ControladorCasosNegocioHijo sAdminC : hijo) {
                try {
                    BC.activateField("TT Etiqueta");
                    BC.activateField("TT Tipo dato");
                    BC.activateField("TT Requerido Flag");
                    BC.activateField("TT CN Id"); // Id de Padre
                    BC.clearToQuery();
                    BC.setSearchSpec("TT Etiqueta", sAdminC.getEtiquetaCampo());
                    BC.setSearchSpec("TT CN Id", RowID);
                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {
                        if (sAdminC.getTipoDato() != null) {
                            if (!BC.getFieldValue("TT Tipo dato").equals(sAdminC.getTipoDato())) {
                                oBCPick = BC.getPicklistBusComp("TT Tipo dato");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Value] ='" + sAdminC.getTipoDato() + "'");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getRequerido() != null) {
                            if (!BC.getFieldValue("TT Requerido Flag").equals(sAdminC.getRequerido())) {
                            }
                        }
                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Formulario de Controlador CN :  " + sAdminC.getEtiquetaCampo());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Formulario de Controlador CN";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowId(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);
                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getEtiquetaCampo() != null) {
                            BC.setFieldValue("TT Etiqueta", sAdminC.getEtiquetaCampo());
                        }

                        if (sAdminC.getTipoDato() != null) {
                            oBCPick = BC.getPicklistBusComp("TT Tipo dato");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value] ='" + sAdminC.getTipoDato() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getRequerido() != null) {
                            BC.setFieldValue("TT Requerido Flag", sAdminC.getRequerido());
                        }

                        BC.setFieldValue("TT CN Id", RowID);

                        BC.writeRecord();

                        System.out.println("Se creo Formulario:  " + sAdminC.getEtiquetaCampo());
                        String mensaje = ("Se creo Formulario:  " + sAdminC.getEtiquetaCampo());
                        String MsgSalida = "Creado Formulario de Controlador CN";

                        this.CargaBitacoraSalidaCreado(sAdminC.getRowId(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);
                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en Hijo de Controlador Casos de Negocio:" + sAdminC.getEtiquetaCampo() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Hijo-Controlador Caso de Negocio";
                    String FlagCarga = "E";
                    String MsgError = "Error en Hijo de Controlador Casos de Negocio:" + sAdminC.getEtiquetaCampo() + "     , con el mensaje:   " + error.replace("'", " ");

                    this.CargaBitacoraSalidaError(sAdminC.getRowId(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgError);

                }
            }
        } catch (SiebelException e) {
            String error = e.getErrorMessage();
            System.out.println("Error en cargaBC, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en cargaBC, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);
        } catch (Exception ex) {
            Logger.getLogger(DAOControladorCasosNegocioImpl.class.getName()).log(Level.SEVERE, null, ex);
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
    public List<ControladorCasosNegocioPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.TT_NIVEL_1 \"Categoria\", A.TT_NIVEL_2 \"Motivo\", A.TT_NIVEL_3 \"SubMotivo\", A.TT_NIVEL_4 \"Solucion\", A.TT_NOM_FORM \"Nombre de Formulario\", A.TT_ACT_TEMPL \"Plantilla de Actividad\", \n"
                + "A.TT_UNIQUE \"CN Unico\", A.TT_EMAIL_FLG \"Email/SMS\", A.TT_ORDER_TYPE \"Orden de Servicio\", A.TT_MODIFIED_BUSINESS_CASE \"Modificar Comentario\", A.TT_ACCION_HIT \"Indicador Accion HIT\", \n"
                + "A.TT_FLG_EQ_TEL \"Copiar Equipos Telefonia\", A.TT_INTERFAZ_REF \"Proceso Interfaz Refresh\", A.TT_MIRADA_TYPE \"Trouble Mirada\", A.TT_MOTIVO_CTE \"Motivo CTE\", A.X_TT_RMD_MONITOR \"Remedy Monitor\", \n"
                + "A.TT_EMAIL_ADM_CN \"CN Administrativo para Mail\", A.TT_ASS_FG_FLAG \"Asociar a Falla General\", A.TT_SUBMOTIVO \"Submotivo FG\", A.TT_SWAT \"Aplica SWAT Team\", A.TT_FOR_DIN_FLG \"Formulario Dinamico\", \n"
                + "A.TT_SR_CST_RECU \"CN Recurrente\", A.TT_TBLSHNG \"TroubleShooting\", A.TT_FLD_REQ_FLG \"Campos Requeridos\", A.TT_OPERA_SUC \"Opera en sucursal\", A.TT_FLG_VISITA_TECNICA \"Visita Tecnica\", \n"
                + "A.TT_CLOSE_ACT_FLG \"Cierre de Actividad\", A.TT_REQ_RPT_PRIMARY \"Restringir Crecion por RPT\", A.TT_NAVIGATOR_FLAG \"Sync Navigator\"\n"
                + "FROM SIEBEL.CX_TT_CTRL_NEG A, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<ControladorCasosNegocioPadre> controladorCasosNegocio = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Padre a procesar.");

            while (rs.next()) {
                ControladorCasosNegocioPadre lista = new ControladorCasosNegocioPadre();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setCategoria(rs.getString("Categoria"));
                lista.setMotivo(rs.getString("Motivo"));
                lista.setSubMotivo(rs.getString("SubMotivo"));
                lista.setSolucion(rs.getString("Solucion"));
                lista.setNombreFormulario(rs.getString("Nombre de Formulario"));
                lista.setPlantillaActividad(rs.getString("Plantilla de Actividad"));
                lista.setCnUnico(rs.getString("CN Unico"));
                lista.setEmailSMS(rs.getString("Email/SMS"));
                lista.setOrdenServicio(rs.getString("Orden de Servicio"));
                lista.setModificarComentario(rs.getString("Modificar Comentario"));
                lista.setIndicadorAccionHIT(rs.getString("Indicador Accion HIT"));
                lista.setCopiarEquiposTelefonia(rs.getString("Copiar Equipos Telefonia"));
                lista.setProcesoInterfazRefresh(rs.getString("Proceso Interfaz Refresh"));
                lista.setTroubleMirada(rs.getString("Trouble Mirada"));
                lista.setMotivoCTE(rs.getString("Motivo CTE"));
                lista.setRemedyMonitor(rs.getString("Remedy Monitor"));
                lista.setCnAdministrativoMail(rs.getString("CN Administrativo para Mail"));
                lista.setAsociarFallaGeneral(rs.getString("Asociar a Falla General"));
                lista.setSubmotivoFG(rs.getString("Submotivo FG"));
                lista.setAplicaSwatTeam(rs.getString("Aplica SWAT Team"));
                lista.setFormularioDinamico(rs.getString("Formulario Dinamico"));
                lista.setCnRecurrente(rs.getString("CN Recurrente"));
                lista.setTroubleShooting(rs.getString("TroubleShooting"));
                lista.setCamposRequeridos(rs.getString("Campos Requeridos"));
                lista.setOperaSucursal(rs.getString("Opera en sucursal"));
                lista.setVistaTecnica(rs.getString("Visita Tecnica"));
                lista.setCierreActividad(rs.getString("Cierre de Actividad"));
                lista.setRestringirCreacionRTP(rs.getString("Restringir Crecion por RPT"));
                lista.setSyncNavigator(rs.getString("Sync Navigator"));

                controladorCasosNegocio.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Categoria"), rs.getString("Motivo"), rs.getString("SubMotivo"), "SE OBTIENEN DATOS", "CONTROLADOR CASOS DE NEGOCIO",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = controladorCasosNegocio.size();
            Boolean conteo = controladorCasosNegocio.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Controlador de Casos de Negocio o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Controlador de Casos de Negocio o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Controlador de Casos de Negocio, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Controlador de Casos de Negocio, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return controladorCasosNegocio;
    }

    @Override
    public List<ControladorCasosNegocioHijo> consultaHijo(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.PAR_ROW_ID \"Id Padre\", B.TT_FIELD \"Etiqueta del Campo\",B.TT_DATA_TYPE \"Tipo de Dato\", B.TT_DATA_REQ_FLG \"Requerido\"\n"
                + "FROM SIEBEL.CX_CTRL_NEG_XM B, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = B.LAST_UPD_BY\n"
                + "AND B.PAR_ROW_ID = '" + IdPadre + "'\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')"
                + "ORDER BY B.LAST_UPD ASC";
        List<ControladorCasosNegocioHijo> controladorCasosNegocio = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {
            System.out.println("Creando Lista de registros Hijo a procesar.");            
            while (rs.next()) {
                ControladorCasosNegocioHijo lista = new ControladorCasosNegocioHijo();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setEtiquetaCampo(rs.getString("Etiqueta del Campo"));
                lista.setTipoDato(rs.getString("Tipo de Dato"));
                lista.setRequerido(rs.getString("Requerido"));

                controladorCasosNegocio.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Etiqueta del Campo"), rs.getString("Tipo de Dato"), rs.getString("Requerido"), "SE OBTIENEN DATOS HIJO", "CONTROLADOR CASOS DE NEGOCIO - FORMULARIOS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = controladorCasosNegocio.size();
            Boolean conteo = controladorCasosNegocio.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Acciones Polizas o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Acciones Polizas o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Acciones Polizas, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Acciones Polizas, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo, con el error:  " + ex);
            throw ex;
        }
        return controladorCasosNegocio;
    }

    @Override
    public List<ControladorCasosNegocioSoloHijo> consultaSoloHijo(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.PAR_ROW_ID \"Id Padre\", B.TT_FIELD \"Etiqueta del Campo\",B.TT_DATA_TYPE \"Tipo de Dato\", B.TT_DATA_REQ_FLG \"Requerido\",\n"
                + "A.TT_NIVEL_1 \"Categoria\", A.TT_NIVEL_2 \"Motivo\", A.TT_NIVEL_3 \"SubMotivo\", A.TT_NIVEL_4 \"Solucion\"\n"
                + "FROM SIEBEL.CX_CTRL_NEG_XM B, SIEBEL.CX_TT_CTRL_NEG A, SIEBEL.S_USER H\n"
                + "WHERE B.PAR_ROW_ID = A.ROW_ID AND H.ROW_ID = B.LAST_UPD_BY\n"
                + "AND B.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY B.LAST_UPD ASC";
        List<ControladorCasosNegocioSoloHijo> controladorCasosNegociosolohijo = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {
            System.out.println("Creando Lista de registros Hijo a procesar.");
            while (rs.next()) {
                ControladorCasosNegocioSoloHijo lista = new ControladorCasosNegocioSoloHijo();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setEtiquetaCampo(rs.getString("Etiqueta del Campo"));
                lista.setTipoDato(rs.getString("Tipo de Dato"));
                lista.setRequerido(rs.getString("Requerido"));

                controladorCasosNegociosolohijo.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Etiqueta del Campo"), rs.getString("Tipo de Dato"), rs.getString("Requerido"), "SE OBTIENEN DATOS HIJO", "CONTROLADOR CASOS DE NEGOCIO - FORMULARIOS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = controladorCasosNegociosolohijo.size();
            Boolean conteo = controladorCasosNegociosolohijo.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Acciones Polizas o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Acciones Polizas o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Acciones Polizas, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Acciones Polizas, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaSoloHijo, con el error:  " + ex);
            throw ex;
        }
        return controladorCasosNegociosolohijo;
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
