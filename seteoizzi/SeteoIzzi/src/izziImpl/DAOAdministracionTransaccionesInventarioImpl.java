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
import interfaces.DAOAdministracionTransaccionesInventario;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import objetos.AdministracionTransaccionesInventario;

/**
 *
 * @author hector.pineda
 */
public class DAOAdministracionTransaccionesInventarioImpl extends ConexionDB implements DAOAdministracionTransaccionesInventario {

    @Override
    public void inserta(List<AdministracionTransaccionesInventario> administracionTransaccionesInventario, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);
        
         Boolean conteopadre = administracionTransaccionesInventario.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);
            

        for (AdministracionTransaccionesInventario sAdmin : administracionTransaccionesInventario) {

            if ("SI".equals(Conectar)) {
                conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                Conectar = "NO";
            }
            try {
                String MsgError = "";

                SiebelBusObject BO = m_dataBean.getBusObject("CV ERP Movements");
                SiebelBusComp BC = BO.getBusComp("CV ERP Movements Admin");
                SiebelBusComp oBCPick = new SiebelBusComp();
                BC.activateField("Product Type");
                BC.activateField("Source InvLoc Type");
                BC.activateField("TT Source RPT");
                BC.activateField("TT Source RPT Compañia");
                BC.activateField("Destination InvLoc Type");
                BC.activateField("TT Dest RPT");
                BC.activateField("TT Dest RPT Compañia");
                BC.activateField("Movement");
                BC.activateField("Report ERP");
                BC.activateField("Cost Center Required");
                BC.clearToQuery();
                BC.setSearchSpec("Product Type", sAdmin.getTipoProducto());
                BC.setSearchSpec("Source InvLoc Type", sAdmin.getTipoUbicacionInvOrigen());
                BC.setSearchSpec("TT Source RPT", sAdmin.getRtpUbicacionInvOrigen());
                BC.setSearchSpec("TT Dest RPT", sAdmin.getRtpUbicacionInvDestino());
                BC.setSearchSpec("Destination InvLoc Type", sAdmin.getTipoUbicacionInvDestino());
                BC.executeQuery(true);
                boolean reg = BC.firstRecord();
                if (reg) {
                    if (sAdmin.getCompUbicacionInvOrigen() != null) {
                        if (!BC.getFieldValue("TT Source RPT Compañia").equals(sAdmin.getCompUbicacionInvOrigen())) {
                            BC.setFieldValue("TT Source RPT Compañia", sAdmin.getCompUbicacionInvOrigen());
                        }
                    }
                    
                    if (sAdmin.getCompUbicacionInvDestino() != null) {
                        if (!BC.getFieldValue("TT Dest RPT Compañia").equals(sAdmin.getCompUbicacionInvDestino())) {
                            BC.setFieldValue("TT Dest RPT Compañia", sAdmin.getCompUbicacionInvDestino());
                        }
                    }

                    if (sAdmin.getMovimiento() != null) {
                        if (!BC.getFieldValue("Movement").equals(sAdmin.getMovimiento())) {
                            oBCPick = BC.getPicklistBusComp("Movement");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value] ='" + sAdmin.getMovimiento()+ "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }
                    }

                    if (sAdmin.getReportarErp() != null) {
                        if (!BC.getFieldValue("Report ERP").equals(sAdmin.getReportarErp())) {
                            BC.setFieldValue("Report ERP", sAdmin.getReportarErp());
                        }
                    }

                    if (sAdmin.getCentroCostosRequerido() != null) {
                        if (!BC.getFieldValue("Cost Center Required").equals(sAdmin.getCentroCostosRequerido())) {
                            BC.setFieldValue("Cost Center Required", sAdmin.getCentroCostosRequerido());
                        }
                    }
                    BC.writeRecord();

                    BC.release();
                    BO.release();

                    System.out.println("Se valida existencia y/o actualizacion Admon. Transacciones Inventario:  " + sAdmin.getTipoProducto() + " : " + sAdmin.getTipoUbicacionInvOrigen()+ " : " + sAdmin.getRtpUbicacionInvOrigen() + " : " + sAdmin.getTipoUbicacionInvDestino());
                    String MsgSalida = "Se valida existencia y/o actualizacion de Admon. Transacciones Inventario";

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgSalida);

                    this.CargaBitacoraSalidaValidado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                } else {
                    BC.newRecord(0);

                    if (sAdmin.getTipoProducto() != null) {
                        oBCPick = BC.getPicklistBusComp("Product Type");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Value] ='" + sAdmin.getTipoProducto()+ "'");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdmin.getTipoUbicacionInvOrigen() != null) {
                        oBCPick = BC.getPicklistBusComp("Source InvLoc Type");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Inventory Location Type] ='" + sAdmin.getTipoUbicacionInvOrigen()+ "'");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdmin.getRtpUbicacionInvOrigen() != null) {
                        oBCPick = BC.getPicklistBusComp("TT Source RPT");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[TT RPT] ='" + sAdmin.getRtpUbicacionInvOrigen()+ "'");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdmin.getCompUbicacionInvOrigen() != null) {
                            BC.setFieldValue("TT Source RPT Compañia", sAdmin.getCompUbicacionInvOrigen());
                    }
                    
                    if (sAdmin.getTipoUbicacionInvDestino() != null) {
                        oBCPick = BC.getPicklistBusComp("Destination InvLoc Type");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Inventory Location Type] ='" + sAdmin.getTipoUbicacionInvDestino()+ "'");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdmin.getRtpUbicacionInvDestino() != null) {
                        oBCPick = BC.getPicklistBusComp("TT Dest RPT");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[TT RPT] ='" + sAdmin.getRtpUbicacionInvDestino()+ "'");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdmin.getCompUbicacionInvDestino() != null) {
                        BC.setFieldValue("TT Dest RPT Compañia", sAdmin.getCompUbicacionInvDestino());
                    }

                    if (sAdmin.getMovimiento() != null) {
                        oBCPick = BC.getPicklistBusComp("Movement");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Value] ='" + sAdmin.getMovimiento()+ "'");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdmin.getReportarErp() != null) {
                        BC.setFieldValue("Report ERP", sAdmin.getReportarErp());
                    }

                    if (sAdmin.getCentroCostosRequerido() != null) {
                        BC.setFieldValue("Cost Center Required", sAdmin.getCentroCostosRequerido());
                    }

                    BC.writeRecord();

                    BC.release();
                    BO.release();

                    System.out.println("Se crea registro de Admon. Transacciones Inventario: " + sAdmin.getTipoProducto() + " : " + sAdmin.getTipoUbicacionInvOrigen()+ " : " + sAdmin.getRtpUbicacionInvOrigen() + " : " + sAdmin.getTipoUbicacionInvDestino());
                    String mensaje = ("Se crea registro de Admon. Transacciones Inventario: " + sAdmin.getTipoProducto() + " : " + sAdmin.getTipoUbicacionInvOrigen()+ " : " + sAdmin.getRtpUbicacionInvOrigen() + " : " + sAdmin.getTipoUbicacionInvDestino());

                    String MsgSalida = "Creado Admon. Transacciones Inventario";
                    this.CargaBitacoraSalidaCreado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(mensaje);

                }
            } catch (SiebelException e) {
                String error = e.getErrorMessage();
                System.out.println("Error en creacion Admon. Transacciones Inventario:  " + sAdmin.getTipoProducto() + " : " + sAdmin.getTipoUbicacionInvOrigen()+ " : " + sAdmin.getRtpUbicacionInvOrigen() + " : " + sAdmin.getTipoUbicacionInvDestino() + "     , con el mensaje:   " + error.replace("'", " "));
                String MsgSalida = "Error al Crear Admon. Transacciones Inventario";
                String FlagCarga = "E";
                String MsgError = "Error en creacion Admon. Transacciones Inventario:  " + sAdmin.getTipoProducto() + " : " + sAdmin.getTipoUbicacionInvOrigen()+ " : " + sAdmin.getRtpUbicacionInvOrigen() + " : " + sAdmin.getTipoUbicacionInvDestino() + "     , con el mensaje:   " + error.replace("'", " ");

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
    public List<AdministracionTransaccionesInventario> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID,A.PROD_TYPE \"Tipo de Producto\", B.NAME \"Tipo Ubic. Inv Origen\", C.RPT \"RPT Ubic. Inv Origen\", C.COMPANIA \"Compañia Ubic Inv Origen\",D.NAME \"Tipo Ubic. Inv Destino\",\n"
                + " E.RPT \"RPT Ubic Inv Destino\", E.COMPANIA \"Compañia Ubic Inv Destino\", A.MOVEMENT \"Movimiento\",a.APPLY \"Reportar a ERP?\", a.COST_CENTER \"Centro de Costos Requerido?\"\n"
                + "FROM SIEBEL.CX_ERP_MOV_ADM A, SIEBEL.S_INVLOC_TYPE B, SIEBEL.CX_RPT_RAZONES C, SIEBEL.S_INVLOC_TYPE D, SIEBEL.CX_RPT_RAZONES E, SIEBEL.S_USER H\n"
                + "WHERE A.SOURCE_TYPE_ID = B.ROW_ID AND A.X_TT_RPT_SRC = C.ROW_ID AND H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.DEST_TYPE_ID = D.ROW_ID AND A.X_TT_RPT_DES = E.ROW_ID\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA E WHERE E.ROW_ID = A.ROW_ID AND E.USUARIO ='" + usuario + "' AND E.VERSION ='" + version + "' AND E.EXTRAER ='" + ambienteExtra + "' AND E.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<AdministracionTransaccionesInventario> administracionTransaccionesInventario = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros a procesar.");

            while (rs.next()) {
                AdministracionTransaccionesInventario lista = new AdministracionTransaccionesInventario();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setTipoProducto(rs.getString("Tipo de Producto"));
                lista.setTipoUbicacionInvOrigen(rs.getString("Tipo Ubic. Inv Origen"));
                lista.setRtpUbicacionInvOrigen(rs.getString("RPT Ubic. Inv Origen"));
                lista.setCompUbicacionInvOrigen(rs.getString("Compañia Ubic Inv Origen"));
                lista.setTipoUbicacionInvDestino(rs.getString("Tipo Ubic. Inv Destino"));
                lista.setRtpUbicacionInvDestino(rs.getString("RPT Ubic Inv Destino"));
                lista.setCompUbicacionInvDestino(rs.getString("Compañia Ubic Inv Destino"));
                lista.setMovimiento(rs.getString("Movimiento"));
                lista.setReportarErp(rs.getString("Reportar a ERP?"));
                lista.setCentroCostosRequerido(rs.getString("Centro de Costos Requerido?"));
                administracionTransaccionesInventario.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Tipo de Producto"), rs.getString("Tipo Ubic. Inv Origen"), rs.getString("RPT Ubic. Inv Origen"), "SE OBTIENEN DATOS",usuario,version,ambienteInser,ambienteExtra);
            }
            int Registros = administracionTransaccionesInventario.size();
            Boolean conteo = administracionTransaccionesInventario.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Admon. Transacciones Inventario o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Admon. Transacciones Inventario o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Admon. Transacciones Inventario, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Admon. Transacciones Inventario, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return administracionTransaccionesInventario;
    }

    private void CargaBitacoraEntrada(String Row_Id, String Val1, String Val2, String Val3, String Seguimiento, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String TipoObjeto = Val1 + " : " + Val2 + " : " + Val3;

        String searchRecordSQL1 = "SELECT ROW_ID FROM SIEBEL.CT_BITACORA WHERE ROW_ID = '" + Row_Id + "' AND USUARIO = '" + usuario + "' AND VERSION = '" + version + "' AND EXTRAER = '" + ambienteExtra + "' AND INSERTAR = '" + ambienteInser + "'";

        try (PreparedStatement ps = conexion.prepareStatement(searchRecordSQL1); ResultSet rs1 = ps.executeQuery()) {
            if (rs1.next()) {
                // sin accion
            } else {
                String setRecordSQL2 = "INSERT INTO SIEBEL.CT_BITACORA  (ROW_ID, TIPO_CATALOGO, TIPO_OBJETO, FLAG_CARGA, SEGUIMIENTO, MENSAJE_ERROR,USUARIO, VERSION, EXTRAER, INSERTAR)\n"
                        + "VALUES('" + Row_Id + "','ADMON. TRANSACCIONES INVENTARIO','" + TipoObjeto + "','N','" + Seguimiento + "','','" + usuario + "','" + version + "','" + ambienteExtra + "','" + ambienteInser + "')";

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
