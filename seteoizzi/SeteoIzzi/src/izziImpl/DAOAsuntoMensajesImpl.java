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
import interfaces.DAOAsuntosMensajes;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import objetos.AsuntoMensajes;

/**
 *
 * @author hector.pineda
 */
public class DAOAsuntoMensajesImpl extends ConexionDB implements DAOAsuntosMensajes {

    @Override
    public void inserta(List<AsuntoMensajes> asuntosMensajes, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);
        
         Boolean conteopadre = asuntosMensajes.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

        for (AsuntoMensajes sAdmin : asuntosMensajes) {

            if ("SI".equals(Conectar)) {
                conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                Conectar = "NO";
            }

            try {

                String MsgError = "";
                SiebelBusObject BO = m_dataBean.getBusObject("TT Subjects");
                SiebelBusComp BC = BO.getBusComp("TT Subjects");
                BC.activateField("Id Mensaje");
                BC.activateField("Izzi Neg");
                BC.activateField("Izzi Res");
                BC.activateField("Wizz Neg");
                BC.activateField("Wizz Res");
                BC.activateField("Process Name");
                BC.activateField("PM Neg");
                BC.activateField("PM Res");
                BC.activateField("PP Res");
                BC.activateField("PK Res");
                BC.clearToQuery();
                BC.setSearchSpec("Id Mensaje", sAdmin.getIdMensaje());
                BC.executeQuery(true);
                boolean reg = BC.firstRecord();
                if (reg) {

                    if (sAdmin.getIzziNeg() != null) {
                        if (!BC.getFieldValue("Izzi Neg").equals(sAdmin.getIzziNeg())) {
                            BC.setFieldValue("Izzi Neg", sAdmin.getIzziNeg());
                        }
                    }
                    if (sAdmin.getIzziRes() != null) {
                        if (!BC.getFieldValue("Izzi Res").equals(sAdmin.getIzziRes())) {
                            BC.setFieldValue("Izzi Res", sAdmin.getIzziRes());
                        }
                    }
                    if (sAdmin.getWizzNeg() != null) {
                        if (!BC.getFieldValue("Wizz Neg").equals(sAdmin.getWizzNeg())) {
                            BC.setFieldValue("Wizz Neg", sAdmin.getWizzNeg());
                        }
                    }
                    if (sAdmin.getWizzRes() != null) {
                        if (!BC.getFieldValue("Wizz Res").equals(sAdmin.getWizzRes())) {
                            BC.setFieldValue("Wizz Res", sAdmin.getWizzRes());
                        }
                    }
                    if (sAdmin.getNombreProceso() != null) {
                        if (!BC.getFieldValue("Process Name").equals(sAdmin.getNombreProceso())) {
                            BC.setFieldValue("Process Name", sAdmin.getNombreProceso());
                        }
                    }
                    if (sAdmin.getPmNeg() != null) {
                        if (!BC.getFieldValue("PM Neg").equals(sAdmin.getPmNeg())) {
                            BC.setFieldValue("PM Neg", sAdmin.getPmNeg());
                        }
                    }
                    if (sAdmin.getPmRes() != null) {
                        if (!BC.getFieldValue("PM Res").equals(sAdmin.getPmRes())) {
                            BC.setFieldValue("PM Res", sAdmin.getPmRes());
                        }
                    }
                    if (sAdmin.getPpRes() != null) {
                        if (!BC.getFieldValue("PP Res").equals(sAdmin.getPpRes())) {
                            BC.setFieldValue("PP Res", sAdmin.getPpRes());
                        }
                    }
                    if (sAdmin.getPocketRes() != null) {
                        if (!BC.getFieldValue("PK Res").equals(sAdmin.getPocketRes())) {
                            BC.setFieldValue("PK Res", sAdmin.getPocketRes());
                        }
                    }
                    BC.writeRecord();

                    BC.release();
                    BO.release();

                    System.out.println("Se valida existencia y/o actualizacion de Asuntos Mensajes con Id de Mensaje:  " + sAdmin.getIdMensaje());
                    String MsgSalida = "Se valida existencia y/o actualizacion de Asuntos Mensajes";
                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgSalida);

                    this.CargaBitacoraSalidaValidado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                } else {
                    BC.newRecord(0);

                    if (sAdmin.getIdMensaje() != null) {
                        BC.setFieldValue("Id Mensaje", sAdmin.getIdMensaje());
                    }

                    if (sAdmin.getIzziNeg() != null) {
                        BC.setFieldValue("Izzi Neg", sAdmin.getIzziNeg());
                    }

                    if (sAdmin.getIzziRes() != null) {
                        BC.setFieldValue("Izzi Res", sAdmin.getIzziRes());
                    }

                    if (sAdmin.getWizzNeg() != null) {
                        BC.setFieldValue("Wizz Neg", sAdmin.getWizzNeg());
                    }

                    if (sAdmin.getWizzRes() != null) {
                        BC.setFieldValue("Wizz Res", sAdmin.getWizzRes());
                    }

                    if (sAdmin.getNombreProceso() != null) {
                        BC.setFieldValue("Process Name", sAdmin.getNombreProceso());
                    }

                    if (sAdmin.getPmNeg() != null) {
                        BC.setFieldValue("PM Neg", sAdmin.getPmNeg());
                    }

                    if (sAdmin.getPmRes() != null) {
                        BC.setFieldValue("PM Res", sAdmin.getPmRes());
                    }

                    if (sAdmin.getPpRes() != null) {
                        BC.setFieldValue("PP Res", sAdmin.getPpRes());
                    }

                    if (sAdmin.getPocketRes() != null) {
                        BC.setFieldValue("PK Res", sAdmin.getPocketRes());
                    }

                    BC.writeRecord();

                    BC.release();
                    BO.release();

                    System.out.println("Se crea registro de Asuntos Mensajes con Id de Mensaje: " + sAdmin.getIdMensaje());
                    String mensaje = ("Se crea registro de Asuntos Mensajes con Id de Mensaje: " + sAdmin.getIdMensaje());

                    String MsgSalida = "Creado Asuntos Mensajes";
                    this.CargaBitacoraSalidaCreado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(mensaje);

                }
            } catch (SiebelException e) {
                String error = e.getErrorMessage();
                System.out.println("Error en creacion Asuntos Mensajes con id de Mensaje:  " + sAdmin.getIdMensaje() + "     , con el mensaje:   " + error.replace("'", " "));
                String MsgSalida = "Error al Crear Asuntos Mensajes";
                String FlagCarga = "E";
                String MsgError = "Error en creacion Asuntos Mensajes con id de Mensaje:  " + sAdmin.getIdMensaje() + "     , con el mensaje:   " + error.replace("'", " ");

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
    public List<AsuntoMensajes> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.ID_MENSAJE \"Id Mensaje\", A.IZZI_NEG \"Izzi Neg\", A.IZZI_RES \"Izzi Nes\", A.WIZZ_NEG \"Wizz Neg\", A.WIZZ_RES \"Wizz Res\", A.PROCESS_NAME \"Nombre Proceso\", A.PM_NEG \"PM Neg\",\n"
                + "A.PM_RES \"PM Res\", A.PP_RES \"PP Res\", A.PPOK_RES \"Pocket Res\"\n"
                + "FROM SIEBEL.CX_TT_SUBJECTS A, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.CREATED BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.CREATED ASC";
        List<AsuntoMensajes> asuntoMensajes = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros a procesar.");

            while (rs.next()) {
                AsuntoMensajes lista = new AsuntoMensajes();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setIdMensaje(rs.getString("Id Mensaje"));
                lista.setIzziRes(rs.getString("Izzi Nes"));
                lista.setIzziNeg(rs.getString("Izzi Neg"));
                lista.setWizzRes(rs.getString("Wizz Res"));
                lista.setWizzNeg(rs.getString("Wizz Neg"));
                lista.setNombreProceso(rs.getString("Nombre Proceso"));
                lista.setPmNeg(rs.getString("PM Neg"));
                lista.setPmRes(rs.getString("PM Res"));
                lista.setPpRes(rs.getString("PP Res"));
                lista.setPocketRes(rs.getString("Pocket Res"));

                asuntoMensajes.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Id Mensaje"), rs.getString("Nombre Proceso"), "SE OBTIENEN DATOS",usuario,version,ambienteInser,ambienteExtra);
            }
            int Registros = asuntoMensajes.size();
            Boolean conteo = asuntoMensajes.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Asuntos Mensajes o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Asuntos Mensajes o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Asuntos Mensajes, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Asuntos Mensajes, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return asuntoMensajes;
    }

    private void CargaBitacoraEntrada(String Row_Id, String Val1, String Val2, String Seguimiento,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String TipoObjeto = Val1 + " : " + Val2;

        String searchRecordSQL1 = "SELECT ROW_ID FROM SIEBEL.CT_BITACORA WHERE ROW_ID = '" + Row_Id + "' AND USUARIO = '" + usuario + "' AND VERSION = '" + version + "' AND EXTRAER = '" + ambienteExtra + "' AND INSERTAR = '" + ambienteInser + "'";

        try (PreparedStatement ps = conexion.prepareStatement(searchRecordSQL1); ResultSet rs1 = ps.executeQuery()) {
            if (rs1.next()) {
                // sin accion
            } else {
                 String setRecordSQL2 = "INSERT INTO SIEBEL.CT_BITACORA  (ROW_ID, TIPO_CATALOGO, TIPO_OBJETO, FLAG_CARGA, SEGUIMIENTO, MENSAJE_ERROR,USUARIO, VERSION, EXTRAER, INSERTAR)\n"
                        + "VALUES('" + Row_Id + "','ASUNTOS MENSAJES','" + TipoObjeto + "','N','" + Seguimiento + "','','" + usuario + "','" + version + "','" + ambienteExtra + "','" + ambienteInser + "')";

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
