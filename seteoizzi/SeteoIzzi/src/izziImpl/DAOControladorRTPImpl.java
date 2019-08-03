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
import interfaces.DAOControladorRTP;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import objetos.ControladorRTP;

/**
 *
 * @author hector.pineda
 */
public class DAOControladorRTPImpl extends ConexionDB implements DAOControladorRTP {

    @Override
    public void inserta(List<ControladorRTP> controladorRTP, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);
        
         Boolean conteopadre = controladorRTP.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);
            
        for (ControladorRTP sAdmin : controladorRTP) {
            if ("SI".equals(Conectar)) {
                conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                Conectar = "NO";
            }

            try {

                String MsgError = "";

                SiebelBusObject BO = m_dataBean.getBusObject("TT Controlador RTP");
                SiebelBusComp BC = BO.getBusComp("TT Controlador RPT");
                SiebelBusComp oBCPick = new SiebelBusComp();
                BC.activateField("TT RPT Name");
                BC.activateField("TT Ctrl RPT");
                BC.activateField("TT Compañia");
                BC.activateField("TT NIR");
                BC.activateField("TT Block TC");
                BC.activateField("TT Division");
                BC.activateField("TT Advanced Payment");
                BC.activateField("TT Forced Term");
                BC.activateField("TT Admission");
                BC.activateField("TT Restrict RPT Dummy");
                BC.activateField("TT izzi Card");
                BC.activateField("TT Pueblo Magico");
                BC.activateField("Parent Id");
                BC.clearToQuery();
                BC.setSearchSpec("TT RPT Name", sAdmin.getRtp());
                BC.setSearchSpec("TT Ctrl RPT", sAdmin.getCodigoRtp());
                BC.setSearchSpec("TT Compañia", sAdmin.getCompañia());
                BC.setSearchSpec("TT NIR", sAdmin.getNir());
                BC.executeQuery(true);
                boolean reg = BC.firstRecord();
                if (reg) { 
                    if (sAdmin.getCodigoRtp() != null) {
                        if (!BC.getFieldValue("TT Ctrl RPT").equals(sAdmin.getCodigoRtp())) {
                            oBCPick = BC.getPicklistBusComp("TT Ctrl RPT");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name] ='" + sAdmin.getCodigoRtp()+ "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();

                            }
                            oBCPick.release();
                        }

                    }
                    if (sAdmin.getCompañia() != null) {
                        if (!BC.getFieldValue("TT Compañia").equals(sAdmin.getCompañia())) {
                            oBCPick = BC.getPicklistBusComp("TT Compañia");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name] ='" + sAdmin.getCompañia()+ "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();

                            }
                            oBCPick.release();
                        }

                    }
                    if (sAdmin.getNir() != null) {
                        if (!BC.getFieldValue("TT NIR").equals(sAdmin.getNir())) {
                            BC.setFieldValue("TT NIR", sAdmin.getNir());

                        }

                    }
                    if (sAdmin.getBloqueoTc() != null) {
                        if (!BC.getFieldValue("TT Block TC").equals(sAdmin.getBloqueoTc())) {
                            BC.setFieldValue("TT Block TC", sAdmin.getBloqueoTc());

                        }

                    }
                    if (sAdmin.getDivision() != null) {
                        if (!BC.getFieldValue("TT Division").equals(sAdmin.getDivision())) {
                            BC.setFieldValue("TT Division", sAdmin.getDivision());

                        }

                    }
                    if (sAdmin.getPagoAnticipado() != null) {
                        if (!BC.getFieldValue("TT Advanced Payment").equals(sAdmin.getPagoAnticipado())) {
                            BC.setFieldValue("TT Advanced Payment", sAdmin.getPagoAnticipado());

                        }

                    }
                    if (sAdmin.getPlazoForzoso() != null) {
                        if (!BC.getFieldValue("TT Forced Term").equals(sAdmin.getPlazoForzoso())) {
                            BC.setFieldValue("TT Forced Term", sAdmin.getPlazoForzoso());

                        }

                    }
                    if (sAdmin.getAdmision() != null) {
                        if (!BC.getFieldValue("TT Admission").equals(sAdmin.getAdmision())) {
                            BC.setFieldValue("TT Admission", sAdmin.getAdmision());

                        }

                    }
                    if (sAdmin.getRestringeDummyProgramar() != null) {
                        if (!BC.getFieldValue("TT Restrict RPT Dummy").equals(sAdmin.getRestringeDummyProgramar())) {
                            BC.setFieldValue("TT Restrict RPT Dummy", sAdmin.getRestringeDummyProgramar());

                        }

                    }
                    if (sAdmin.getTarjeta() != null) {
                        if (!BC.getFieldValue("TT izzi Card").equals(sAdmin.getTarjeta())) {
                            oBCPick = BC.getPicklistBusComp("TT izzi Card");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name] ='" + sAdmin.getTarjeta()+ "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();

                            }
                            oBCPick.release();
                        }

                    }
                    if (sAdmin.getPuebloMagico() != null) {
                        if (!BC.getFieldValue("TT Pueblo Magico").equals(sAdmin.getPuebloMagico())) {
                            BC.setFieldValue("TT Pueblo Magico", sAdmin.getPuebloMagico());

                        }

                    }
                    BC.writeRecord();
                    BC.release();
                    BO.release();

                    System.out.println("Se valida existencia y/o actualizacion Controlador RPT:  " + sAdmin.getRtp());
                    String MsgSalida = "Se valida existencia y/o actualizacion de Controlador RPT";
                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgSalida);

                    this.CargaBitacoraSalidaValidado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                } else {
                    BC.newRecord(0);

                    if (sAdmin.getRtp() != null) {
                        BC.setFieldValue("TT RPT Name", sAdmin.getRtp());
                    }

                    if (sAdmin.getCodigoRtp() != null) {
                        oBCPick = BC.getPicklistBusComp("TT Ctrl RPT");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Name] ='" + sAdmin.getCodigoRtp()+ "'");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }
                    

                    if (sAdmin.getCompañia() != null) {
                        oBCPick = BC.getPicklistBusComp("TT Compañia");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Name] ='" + sAdmin.getCompañia()+ "'");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdmin.getNir() != null) {
                        BC.setFieldValue("TT NIR", sAdmin.getNir());
                    }

                    if (sAdmin.getBloqueoTc() != null) {
                        BC.setFieldValue("TT Block TC", sAdmin.getBloqueoTc());
                    }

                    if (sAdmin.getDivision() != null) {
                        BC.setFieldValue("TT Division", sAdmin.getDivision());
                    }

                    if (sAdmin.getPagoAnticipado() != null) {
                        BC.setFieldValue("TT Advanced Payment", sAdmin.getPagoAnticipado());
                    }

                    if (sAdmin.getPlazoForzoso() != null) {
                        BC.setFieldValue("TT Forced Term", sAdmin.getPlazoForzoso());
                    }

                    if (sAdmin.getAdmision() != null) {
                        BC.setFieldValue("TT Admission", sAdmin.getAdmision());
                    }

                    if (sAdmin.getRestringeDummyProgramar() != null) {
                        BC.setFieldValue("TT Restrict RPT Dummy", sAdmin.getRestringeDummyProgramar());
                    }

                    if (sAdmin.getTarjeta() != null) {
                        oBCPick = BC.getPicklistBusComp("TT izzi Card");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Name]='" + sAdmin.getTarjeta() + "' ");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdmin.getPuebloMagico() != null) {
                        BC.setFieldValue("TT Pueblo Magico", sAdmin.getPuebloMagico());
                    }

                    BC.writeRecord();
                    BC.release();
                    BO.release();
                    
                    System.out.println("Se crea registro de Controlador RPT: " + sAdmin.getRtp());
                    String mensaje = ("Se crea registro de Controlador RPT: " + sAdmin.getRtp());
                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(mensaje);

                    String MsgSalida = "Creado Catalogo Producto Forzoso";
                    this.CargaBitacoraSalidaCreado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                }

            } catch (SiebelException e) {
                String error = e.getErrorMessage();
                System.out.println("Error en creacion Controlador RPT:  " + sAdmin.getRtp() + "     , con el mensaje:   " + error.replace("'", " "));
                String MsgSalida = "Error al Crear Controlador RPT";
                String FlagCarga = "E";
                String MsgError = "Error en creacion Controlador RPT:  " + sAdmin.getRtp() + "     , con el mensaje:   " + error.replace("'", " ");

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
    public List<ControladorRTP> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.TT_RPT_NAME \"RPT\", A.TT_CTRL_RPT \"Codigo RPT\", A.TT_COMPANY \"Compañía\", A.TT_NIR \"NIR\", A.TT_BLOCK_TC \"Bloqueo de TC\", A.TT_DIVISION \"Division\", A.TT_ADVANCE_PAYMENT \"Pago Anticipado\", \n"
                + "A.TT_FORCED_TERM \"Plazo Forzoso\", A.TT_ADMISSION \"Admision\", A.TT_RESTRICT_RPT_DUMMY \"Restringe Dummy al programar\", A.TT_IZZI_CARD \"Tarjeta\", A.TT_PUEBLO_MAGICO \"Pueblo Magico\",\n"
                + "A.TT_PR_ORDER_TYPE_ID \"Restriccion Tipo de Ordenes\"\n"
                + "FROM SIEBEL.CX_TT_CTRL_RPT A, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = A.LAST_UPD_BY AND H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<ControladorRTP> controladorRTP = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {
            System.out.println("Creando Lista de registros a procesar.");
            while (rs.next()) {
                ControladorRTP lista = new ControladorRTP();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setRtp(rs.getString("RPT"));
                lista.setCodigoRtp(rs.getString("Codigo RPT"));
                lista.setCompañia(rs.getString("Compañía"));
                lista.setNir(rs.getString("NIR"));
                lista.setBloqueoTc(rs.getString("Bloqueo de TC"));
                lista.setDivision(rs.getString("Division"));
                lista.setPagoAnticipado(rs.getString("Pago Anticipado"));
                lista.setPlazoForzoso(rs.getString("Plazo Forzoso"));
                lista.setAdmision(rs.getString("Admision"));
                lista.setRestringeDummyProgramar(rs.getString("Restringe Dummy al programar"));
                lista.setTarjeta(rs.getString("Tarjeta"));
                lista.setPuebloMagico(rs.getString("Pueblo Magico"));
                lista.setRestriccionTipoOrdenes(rs.getString("Restriccion Tipo de Ordenes"));
                controladorRTP.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("RPT"), rs.getString("Codigo RPT"), rs.getString("Compañía"), "SE OBTIENEN DATOS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = controladorRTP.size();
            Boolean conteo = controladorRTP.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Controlador RPT o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Controlador RPT o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Controlador RPT, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Controlador RPT, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return controladorRTP;
    }

    private void CargaBitacoraEntrada(String Row_Id, String Val1, String Val2, String Val3, String Seguimiento,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String TipoObjeto = Val1 + " : " + Val2 + " : " + Val3;

        String searchRecordSQL1 = "SELECT ROW_ID FROM SIEBEL.CT_BITACORA WHERE ROW_ID = '" + Row_Id + "' AND USUARIO = '" + usuario + "' AND VERSION = '" + version + "' AND EXTRAER = '" + ambienteExtra + "' AND INSERTAR = '" + ambienteInser + "'";

        try (PreparedStatement ps = conexion.prepareStatement(searchRecordSQL1); ResultSet rs1 = ps.executeQuery()) {
            if (rs1.next()) {
                // sin accion
            } else {
                String setRecordSQL2 = "INSERT INTO SIEBEL.CT_BITACORA  (ROW_ID, TIPO_CATALOGO, TIPO_OBJETO, FLAG_CARGA, SEGUIMIENTO, MENSAJE_ERROR,USUARIO, VERSION, EXTRAER, INSERTAR)\n"
                        + "VALUES('" + Row_Id + "','CONTROLADOR RPT','" + TipoObjeto + "','N','" + Seguimiento + "','','" + usuario + "','" + version + "','" + ambienteExtra + "','" + ambienteInser + "')";

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
