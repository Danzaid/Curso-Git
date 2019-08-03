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
import interfaces.DAOEventosTiempoEjecucion;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import objetos.EventosTiempoEjecucion;

/**
 *
 * @author hector.pineda
 */
public class DAOEventosTiempoEjecucionImpl extends ConexionDB implements DAOEventosTiempoEjecucion {

    @Override
    public void inserta(List<EventosTiempoEjecucion> eventosTiempoEjecucion, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);
        
         Boolean conteopadre = eventosTiempoEjecucion.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);
            
            
        for (EventosTiempoEjecucion sAdmin : eventosTiempoEjecucion) {
            if ("SI".equals(Conectar)) {
                conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                Conectar = "NO";
            }
            try {

                String MsgError = "";

                SiebelBusObject BO = m_dataBean.getBusObject("Personalization Events");
                SiebelBusComp BC = BO.getBusComp("Personalization Event");
                SiebelBusComp oBCPick = new SiebelBusComp();

                BC.activateField("Event Def Name");
                BC.activateField("Sequence");
                BC.activateField("Object Type");
                BC.activateField("Object Name");
                BC.activateField("Event");
                BC.activateField("Sub-Event");
                BC.activateField("Condition Expression");
                BC.activateField("Action Set Name");
                BC.clearToQuery();
                BC.setSearchSpec("Object Type", sAdmin.getTipoObjeto());
                BC.setSearchSpec("Object Name", sAdmin.getObjectName());
                BC.setSearchSpec("Event", sAdmin.getEvento());
                BC.setSearchSpec("Sub-Event", sAdmin.getSubevento());
                BC.executeQuery(true);
                boolean reg = BC.firstRecord();
                if (reg) {
                    if (sAdmin.getNombre() != null) {
                        if (!BC.getFieldValue("Event Def Name").equals(sAdmin.getNombre())) {
                            BC.setFieldValue("Event Def Name", sAdmin.getNombre());

                        }
                    }

                    if (sAdmin.getSecuencia() != null) {
                        if (!BC.getFieldValue("Sequence").equals(sAdmin.getSecuencia())) {
                            BC.setFieldValue("Sequence", sAdmin.getSecuencia());

                        }
                    }

                    if (sAdmin.getExprecionCondicional() != null) {
                        if (!BC.getFieldValue("Condition Expression").equals(sAdmin.getExprecionCondicional())) {
                            BC.setFieldValue("Condition Expression", sAdmin.getExprecionCondicional());

                        }
                    }

                    if (sAdmin.getNombreJuegoAcciones() != null) {
                        if (!BC.getFieldValue("Action Set Name").equals(sAdmin.getNombreJuegoAcciones())) {
                            oBCPick = BC.getPicklistBusComp("Action Set Name");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name]='" + sAdmin.getNombreJuegoAcciones() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();

                            }
                            oBCPick.release();
                        }
                    }
                    BC.writeRecord();

                    System.out.println("Se valida existencia y/o actualizacion Eventos de Tiempo de Ejecucion:  " + sAdmin.getTipoObjeto()+ " : " + sAdmin.getObjectName());
                    String MsgSalida = "Se valida existencia y/o actualizacion de Eventos de Tiempo de Ejecucion";
                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgSalida);

                    this.CargaBitacoraSalidaValidado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                    BC.release();
                    BO.release();

                } else {
                    BC.newRecord(0);

                    if (sAdmin.getNombre() != null) {
                        oBCPick = BC.getPicklistBusComp("Event Def Name");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Name]='" + sAdmin.getNombre() + "' ");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick = null;
                    }

                    if (sAdmin.getSecuencia() != null) {
                        BC.setFieldValue("Sequence", sAdmin.getSecuencia());
                    }

                    if (sAdmin.getTipoObjeto() != null) {
                        oBCPick = BC.getPicklistBusComp("Object Type");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Value]='" + sAdmin.getTipoObjeto() + "' ");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdmin.getObjectName() != null) {
                        oBCPick = BC.getPicklistBusComp("Object Name");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Name]='" + sAdmin.getObjectName() + "' ");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdmin.getEvento() != null) {
                        oBCPick = BC.getPicklistBusComp("Event");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Value]='" + sAdmin.getEvento() + "' ");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdmin.getSubevento() != null) {
                        BC.setFieldValue("Sub-Event", sAdmin.getSubevento());
                    }

                    if (sAdmin.getExprecionCondicional() != null) {
                        BC.setFieldValue("Condition Expression", sAdmin.getExprecionCondicional());
                    }

                    if (sAdmin.getNombreJuegoAcciones() != null) {
                        oBCPick = BC.getPicklistBusComp("Action Set Name");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Name]='" + sAdmin.getNombreJuegoAcciones() + "' ");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    BC.writeRecord();

                    System.out.println("Se crea registro de Eventos de Tiempo de Ejecucion: " + sAdmin.getTipoObjeto()+ " : " + sAdmin.getObjectName());
                    String mensaje = ("Se crea registro de Eventos de Tiempo de Ejecucion: " + sAdmin.getTipoObjeto()+ " : " + sAdmin.getObjectName());

                    String MsgSalida = "Creado Eventos de Tiempo de Ejecucion";
                    this.CargaBitacoraSalidaCreado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(mensaje);

                    BC.release();
                    BO.release();
                }
            } catch (SiebelException e) {
                String error = e.getErrorMessage();
                System.out.println("Error en creacion Eventos de Tiempo de Ejecucion:  " + sAdmin.getTipoObjeto()+ " : " + sAdmin.getObjectName() + "     , con el mensaje:   " + error.replace("'", " "));
                String MsgSalida = "Error al Crear Eventos de Tiempo de Ejecucion";
                String FlagCarga = "E";
                String MsgError = "Error en creacion Eventos de Tiempo de Ejecucion:  " + sAdmin.getTipoObjeto()+ " : " + sAdmin.getObjectName() + "     , con el mensaje:   " + error.replace("'", " ");

                this.CargaBitacoraSalidaError(sAdmin.getRowId(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);
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
    public List<EventosTiempoEjecucion> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, B.NAME \"Nombre\", A.EVT_SEQ_NUM \"Secuencia\", A.OBJ_TYPE_CD \"Tipo de objeto\", A.OBJ_NAME \"ObjectName\", A.EVT_NAME \"Evento\", A.EVT_SUB_NAME \"Subevento\",\n"
                + " A.ACTN_COND_EXPR \"Expresión condicional\", C.NAME \"Nombre del juego de acciones\", 'N' AS FLAG_CARGA, NULL AS DESCRIPCION\n"
                + "FROM SIEBEL.S_CT_EVENT A, SIEBEL.S_CT_EVENT_DEF B, SIEBEL.S_CT_ACTION_SET C, SIEBEL.S_USER H\n"
                + "WHERE A.EVT_DEF_ID = B.ROW_ID(+) AND A.CT_ACTN_SET_ID = C.ROW_ID AND H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<EventosTiempoEjecucion> eventosTiempoEjecucion = new LinkedList();

        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {
            System.out.println("Creando Lista de registros a procesar.");
            while (rs.next()) {
                EventosTiempoEjecucion lista = new EventosTiempoEjecucion();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setSecuencia(rs.getString("Secuencia"));
                lista.setTipoObjeto(rs.getString("Tipo de objeto"));
                lista.setObjectName(rs.getString("ObjectName"));
                lista.setEvento(rs.getString("Evento"));
                lista.setSubevento(rs.getString("Subevento"));
                lista.setExprecionCondicional(rs.getString("Expresión condicional"));
                lista.setNombreJuegoAcciones(rs.getString("Nombre del juego de acciones"));
                eventosTiempoEjecucion.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre"), rs.getString("Tipo de objeto"), rs.getString("Evento"), "SE OBTIENEN DATOS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = eventosTiempoEjecucion.size();
            Boolean conteo = eventosTiempoEjecucion.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Eventos de Tiempo de Ejecucion o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Eventos de Tiempo de Ejecucion o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Eventos de Tiempo de Ejecucion, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Eventos de Tiempo de Ejecucion, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return eventosTiempoEjecucion;
    }

    private void CargaBitacoraEntrada(String Row_Id, String Val1, String Val2, String Val3, String Seguimiento,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String TipoObjeto = Val1 + " : " + Val2 + " : " + Val3;

        String searchRecordSQL1 = "SELECT ROW_ID FROM SIEBEL.CT_BITACORA WHERE ROW_ID = '" + Row_Id + "' AND USUARIO = '" + usuario + "' AND VERSION = '" + version + "' AND EXTRAER = '" + ambienteExtra + "' AND INSERTAR = '" + ambienteInser + "'";

        try (PreparedStatement ps = conexion.prepareStatement(searchRecordSQL1); ResultSet rs1 = ps.executeQuery()) {
            if (rs1.next()) {
                // sin accion
            } else {
                String setRecordSQL2 = "INSERT INTO SIEBEL.CT_BITACORA  (ROW_ID, TIPO_CATALOGO, TIPO_OBJETO, FLAG_CARGA, SEGUIMIENTO, MENSAJE_ERROR,USUARIO, VERSION, EXTRAER, INSERTAR)\n"
                        + "VALUES('" + Row_Id + "','EVENTOS DE TIEMPO DE EJECUCION','" + TipoObjeto + "','N','" + Seguimiento + "','','" + usuario + "','" + version + "','" + ambienteExtra + "','" + ambienteInser + "')";

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
