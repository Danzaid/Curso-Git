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
import interfaces.DAOOficinasOtrosMediosPago;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import objetos.OficinasOtrosMediosPago;

/**
 *
 * @author hector.pineda
 */
public class DAOOficinasOtrosMediosPagoImpl extends ConexionDB implements DAOOficinasOtrosMediosPago {

    @Override
    public void inserta(List<OficinasOtrosMediosPago> oficinasOtrosMediosPago, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);
        
         Boolean conteopadre = oficinasOtrosMediosPago.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);
            
        for (OficinasOtrosMediosPago sAdmin : oficinasOtrosMediosPago) {
            if ("SI".equals(Conectar)) {
                conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                Conectar = "NO";
            }
            try {
                String MsgError = "";
                SiebelBusObject BO = m_dataBean.getBusObject("TT Data Administration");
                SiebelBusComp BC = BO.getBusComp("TT Offices");
                SiebelBusComp oBCPick = new SiebelBusComp();
                BC.activateField("TT Active");
                BC.activateField("TT Displayed");
                BC.activateField("TT MID Code");
                BC.activateField("TT Type");
                BC.activateField("TT Office Name");
                BC.activateField("TT City");
                BC.activateField("TT Office Address");
                BC.activateField("TT Referencia");
                BC.activateField("TT RZ");
                BC.activateField("TT Comments");
                BC.activateField("TT Movements");
                BC.activateField("TT Allow Dollar Payment");
                BC.activateField("TT Merchant 1");
                BC.activateField("TT Merchant 2");
                BC.activateField("TT Merchant 3");
                BC.activateField("TT Is CallCenter");
                BC.clearToQuery();
                BC.setSearchExpr("[TT Type] ='" + sAdmin.getTipo() + "' AND [TT Office Name] = '" + sAdmin.getOficinaOtroMedioPago()
                        + "' AND [TT City] = '" + sAdmin.getRtp() + "' ");
                BC.executeQuery(true);
                boolean reg = BC.firstRecord();
                if (reg) {
                    if (sAdmin.getActivo() != null) {
                        if (!BC.getFieldValue("TT Active").equals(sAdmin.getActivo())) {
                            BC.setFieldValue("TT Active", sAdmin.getActivo());
                        }
                    }

                    if (sAdmin.getVisible() != null) {
                        if (!BC.getFieldValue("TT Displayed").equals(sAdmin.getVisible())) {
                            BC.setFieldValue("TT Displayed", sAdmin.getVisible());
                        }
                    }

                    if (sAdmin.getCodigoMID() != null) {
                        if (!BC.getFieldValue("TT MID Code").equals(sAdmin.getCodigoMID())) {
                            BC.setFieldValue("TT MID Code", sAdmin.getCodigoMID());
                        }
                    }

                    if (sAdmin.getDireccionOficina() != null) {
                        if (!BC.getFieldValue("TT Office Address").equals(sAdmin.getDireccionOficina())) {
                            BC.setFieldValue("TT Office Address", sAdmin.getDireccionOficina());
                        }
                    }

                    if (sAdmin.getReferencia() != null) {
                        if (!BC.getFieldValue("TT Referencia").equals(sAdmin.getReferencia())) {
                            BC.setFieldValue("TT Referencia", sAdmin.getReferencia());
                        }
                    }

                    if (sAdmin.getRazonSocial() != null) {
                        if (!BC.getFieldValue("TT RZ").equals(sAdmin.getRazonSocial())) {
                            BC.setFieldValue("TT RZ", sAdmin.getRazonSocial());
                        }
                    }

                    if (sAdmin.getComentarios() != null) {
                        if (!BC.getFieldValue("TT Comments").equals(sAdmin.getComentarios())) {
                            BC.setFieldValue("TT Comments", sAdmin.getComentarios());
                        }
                    }

                    if (sAdmin.getTramitesPosibles() != null) {
                        if (!BC.getFieldValue("TT Active").equals(sAdmin.getTramitesPosibles())) {
                            BC.setFieldValue("TT Movements", sAdmin.getTramitesPosibles());
                        }
                    }

                    if (sAdmin.getPagoDolares() != null) {
                        if (!BC.getFieldValue("TT Allow Dollar Payment").equals(sAdmin.getPagoDolares())) {
                            BC.setFieldValue("TT Allow Dollar Payment", sAdmin.getPagoDolares());
                        }
                    }

                    if (sAdmin.getMerchant1() != null) {
                        if (!BC.getFieldValue("TT Merchant 1").equals(sAdmin.getMerchant1())) {
                            BC.setFieldValue("TT Merchant 1", sAdmin.getMerchant1());
                        }
                    }

                    if (sAdmin.getMerchant2() != null) {
                        if (!BC.getFieldValue("TT Merchant 2").equals(sAdmin.getMerchant2())) {
                            BC.setFieldValue("TT Merchant 2", sAdmin.getMerchant2());
                        }
                    }

                    if (sAdmin.getMerchant3() != null) {
                        if (!BC.getFieldValue("TT Merchant 3").equals(sAdmin.getMerchant3())) {
                            BC.setFieldValue("TT Merchant 3", sAdmin.getMerchant3());
                        }
                    }

                    if (sAdmin.getCallCenter() != null) {
                        if (!BC.getFieldValue("TT Is CallCenter").equals(sAdmin.getCallCenter())) {
                            BC.setFieldValue("TT Is CallCenter", sAdmin.getCallCenter());
                        }
                    }
                    BC.writeRecord();
                    BC.release();
                    BO.release();

                    System.out.println("Se valida existencia y/o actualizacion Oficinas-Otros Medios de Pago:  " + sAdmin.getTipo() + " : " + sAdmin.getOficinaOtroMedioPago() + " : " + sAdmin.getRtp());
                    String MsgSalida = "Se valida existencia y/o actualizacion de Oficinas-Otros Medios de Pago";
                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgSalida);

                    this.CargaBitacoraSalidaValidado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                } else {
                    BC.newRecord(0);
                    if (sAdmin.getActivo() != null) {
                        BC.setFieldValue("TT Active", sAdmin.getActivo());
                    }

                    if (sAdmin.getVisible() != null) {
                        BC.setFieldValue("TT Displayed", sAdmin.getVisible());
                    }

                    if (sAdmin.getCodigoMID() != null) {
                        BC.setFieldValue("TT MID Code", sAdmin.getCodigoMID());
                    }

                    if (sAdmin.getDireccionOficina() != null) {
                        oBCPick = BC.getPicklistBusComp("TT Type");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchSpec("Value", sAdmin.getTipo());
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }
                    if (sAdmin.getDireccionOficina() != null) {
                        oBCPick = BC.getPicklistBusComp("TT Office Name");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Value]='" + sAdmin.getOficinaOtroMedioPago() + "' ");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }
                    if (sAdmin.getDireccionOficina() != null) {
                        oBCPick = BC.getPicklistBusComp("TT City");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchSpec("TT RPT", sAdmin.getRtp());
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }
                    if (sAdmin.getDireccionOficina() != null) {
                        BC.setFieldValue("TT Office Address", sAdmin.getDireccionOficina());
                    }

                    if (sAdmin.getReferencia() != null) {
                        BC.setFieldValue("TT Referencia", sAdmin.getReferencia());
                    }

                    if (sAdmin.getRazonSocial() != null) {
                        BC.setFieldValue("TT RZ", sAdmin.getRazonSocial());
                    }

                    if (sAdmin.getComentarios() != null) {
                        BC.setFieldValue("TT Comments", sAdmin.getComentarios());
                    }

                    if (sAdmin.getTramitesPosibles() != null) {
                        BC.setFieldValue("TT Movements", sAdmin.getTramitesPosibles());
                    }

                    if (sAdmin.getPagoDolares() != null) {
                        BC.setFieldValue("TT Allow Dollar Payment", sAdmin.getPagoDolares());
                    }

                    if (sAdmin.getMerchant1() != null) {
                        BC.setFieldValue("TT Merchant 1", sAdmin.getMerchant1());
                    }

                    if (sAdmin.getMerchant2() != null) {
                        BC.setFieldValue("TT Merchant 2", sAdmin.getMerchant2());
                    }

                    if (sAdmin.getMerchant3() != null) {
                        BC.setFieldValue("TT Merchant 3", sAdmin.getMerchant3());
                    }

                    if (sAdmin.getCallCenter() != null) {
                        BC.setFieldValue("TT Is CallCenter", sAdmin.getCallCenter());
                    }

                    BC.writeRecord();
                    BC.release();
                    BO.release();

                    System.out.println("Se crea registro de Oficinas-Otros Medios de Pago: " + sAdmin.getTipo() + " : " + sAdmin.getOficinaOtroMedioPago() + " : " + sAdmin.getRtp());
                    String mensaje = ("Se crea registro de Oficinas-Otros Medios de Pago: " + sAdmin.getTipo() + " : " + sAdmin.getOficinaOtroMedioPago() + " : " + sAdmin.getRtp());

                    String MsgSalida = "Creado Oficinas-Otros Medios de Pago";
                    this.CargaBitacoraSalidaCreado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(mensaje);

                }

            } catch (SiebelException e) {
                String error = e.getErrorMessage();
                System.out.println("Error en creacion Oficinas-Otros Medios de Pago:  " + sAdmin.getTipo() + " : " + sAdmin.getOficinaOtroMedioPago() + " : " + sAdmin.getRtp() + "     , con el mensaje:   " + error.replace("'", " "));
                String MsgSalida = "Error al Crear Oficinas-Otros Medios de Pago";
                String FlagCarga = "E";
                String MsgError = "Error en creacion Oficinas-Otros Medios de Pago:  " + sAdmin.getTipo() + " : " + sAdmin.getOficinaOtroMedioPago() + " : " + sAdmin.getRtp() + "     , con el mensaje:   " + error.replace("'", " ");

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
    public List<OficinasOtrosMediosPago> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.TT_ACTIVE \"Activo\", A.TT_DISPLAYED \"Visible\", A.TT_MID_CODE \"Codigo MID\", A.TT_TYPE \"Tipo\", A.TT_OFFICE_NAME \"Oficina/Otro Medio de Pago\", A.TT_CITY \"RTP\", A.TT_OFFICE_ADDRESS \"Direccion de la Oficina\", \n"
                + "A.TT_ADDR_REFERENCE \"Referencia\", A.TT_RZ \"Razon Social\", A.TT_COMMENTS \"Comentarios\", A.TT_OPERATIONS \"Tramites Posibles\", A.TT_ALLOW_DOLLAR \"Pago Dolares\", A.TT_MERCHANT_1 \"Merchant 1\",\n"
                + " A.TT_MERCHANT_2 \"Merchant 2\", A.TT_MERCHANT_3 \"Merchant 3\", A.TT_ISCALLCENTER \"Call Center\",  'N' AS FLAG_CARGA, NULL AS DESCRIPCION\n"
                + "FROM SIEBEL.CX_TT_OFFICE A, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<OficinasOtrosMediosPago> oficinasOtrosMediosPago = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {
            System.out.println("Creando Lista de registros a procesar.");
            while (rs.next()) {
                OficinasOtrosMediosPago lista = new OficinasOtrosMediosPago();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setActivo(rs.getString("Activo"));
                lista.setVisible(rs.getString("Visible"));
                lista.setCodigoMID(rs.getString("Codigo MID"));
                lista.setTipo(rs.getString("Tipo"));
                lista.setOficinaOtroMedioPago(rs.getString("Oficina/Otro Medio de Pago"));
                lista.setRtp(rs.getString("RTP"));
                lista.setDireccionOficina(rs.getString("Direccion de la Oficina"));
                lista.setReferencia(rs.getString("Referencia"));
                lista.setRazonSocial(rs.getString("Razon Social"));
                lista.setComentarios(rs.getString("Comentarios"));
                lista.setTramitesPosibles(rs.getString("Tramites Posibles"));
                lista.setPagoDolares(rs.getString("Pago Dolares"));
                lista.setMerchant1(rs.getString("Merchant 1"));
                lista.setMerchant2(rs.getString("Merchant 2"));
                lista.setMerchant3(rs.getString("Merchant 3"));
                lista.setCallCenter(rs.getString("Call Center"));
                oficinasOtrosMediosPago.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Tipo"), rs.getString("Oficina/Otro Medio de Pago"), rs.getString("RTP"), "SE OBTIENEN DATOS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = oficinasOtrosMediosPago.size();
            Boolean conteo = oficinasOtrosMediosPago.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Oficinas-Otros Medios de Pago o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Oficinas-Otros Medios de Pago o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Oficinas-Otros Medios de Pago, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Oficinas-Otros Medios de Pago, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return oficinasOtrosMediosPago;
    }

    private void CargaBitacoraEntrada(String Row_Id, String Val1, String Val2, String Val3, String Seguimiento,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String TipoObjeto = Val1 + " : " + Val2 + " : " + Val3;

        String searchRecordSQL1 = "SELECT ROW_ID FROM SIEBEL.CT_BITACORA WHERE ROW_ID = '" + Row_Id + "' AND USUARIO = '" + usuario + "' AND VERSION = '" + version + "' AND EXTRAER = '" + ambienteExtra + "' AND INSERTAR = '" + ambienteInser + "'";

        try (PreparedStatement ps = conexion.prepareStatement(searchRecordSQL1); ResultSet rs1 = ps.executeQuery()) {
            if (rs1.next()) {
                // sin accion
            } else {
                String setRecordSQL2 = "INSERT INTO SIEBEL.CT_BITACORA  (ROW_ID, TIPO_CATALOGO, TIPO_OBJETO, FLAG_CARGA, SEGUIMIENTO, MENSAJE_ERROR,USUARIO, VERSION, EXTRAER, INSERTAR)\n"
                        + "VALUES('" + Row_Id + "','OFICINAS - OTROS MEDIOS DE PAGO','" + TipoObjeto + "','N','" + Seguimiento + "','','" + usuario + "','" + version + "','" + ambienteExtra + "','" + ambienteInser + "')";

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
