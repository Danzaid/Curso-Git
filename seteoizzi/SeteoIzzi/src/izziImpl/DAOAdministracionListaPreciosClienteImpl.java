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
import interfaces.DAOAdministracionListaPreciosCliente;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import objetos.AdministracionListaPreciosCliente;

/**
 *
 * @author hector.pineda
 */
public class DAOAdministracionListaPreciosClienteImpl extends ConexionDB implements DAOAdministracionListaPreciosCliente {

    @Override
    public void inserta(List<AdministracionListaPreciosCliente> administracionListaPrecios, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);
        
         Boolean conteopadre = administracionListaPrecios.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);
            
            
        for (AdministracionListaPreciosCliente sIndus : administracionListaPrecios) {
            if ("SI".equals(Conectar)) {
                conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                Conectar = "NO";
            }
            try {
                String MsgError = "";
                SiebelBusObject BO = m_dataBean.getBusObject("CV Account Price List BusObj");
                SiebelBusComp BC = BO.getBusComp("CV Account Price List BusComp");
                SiebelBusComp oBCPick = new SiebelBusComp();
                BC.activateField("Account Type");
                BC.activateField("Account Sub Type");
                BC.activateField("Digital Area");
                BC.activateField("Price List Name");
                BC.clearToQuery();
                BC.setSearchExpr("[Account Type] ='" + sIndus.getTipoCuenta() + "' AND [Account Sub Type] = '" + sIndus.getSubtipoCuenta() + "' AND [Price List Name] = '" + sIndus.getListaPrecios() + "'");
                BC.executeQuery(true);
                boolean reg = BC.firstRecord();
                if (reg) {

                    if (sIndus.getZonaDijitalizada() != null) {
                        if (!BC.getFieldValue("Digital Area").equals(sIndus.getZonaDijitalizada())) {
                            BC.setFieldValue("Digital Area", sIndus.getZonaDijitalizada());

                        }
                    }
                    BC.writeRecord();
                    BC.release();
                    BO.release();

                    System.out.println("Se valida existencia y/o actualizacion de Lista de Precios por Cliente:  " + sIndus.getTipoCuenta());
                    String MsgSalida = "Se valida existencia y/o actualizacion de Lista de Precios por Cliente";

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgSalida);

                    this.CargaBitacoraSalidaValidado(sIndus.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);
                } else {
                    BC.newRecord(0);

                    if (sIndus.getTipoCuenta() != null) {
                        oBCPick = BC.getPicklistBusComp("Account Type");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Value]='" + sIndus.getTipoCuenta() + "' ");
                        oBCPick.executeQuery(false);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sIndus.getSubtipoCuenta() != null) {
                        oBCPick = BC.getPicklistBusComp("Account Sub Type");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Parent] ='" + sIndus.getTipoCuenta() + "' AND [Value] = '" + sIndus.getSubtipoCuenta() + "' ");
                        oBCPick.executeQuery(false);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sIndus.getZonaDijitalizada() != null) {
                        BC.setFieldValue("Digital Area", sIndus.getZonaDijitalizada());
                    }

                    if (sIndus.getListaPrecios() != null) {
                        oBCPick = BC.getPicklistBusComp("Price List Name");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Name]='" + sIndus.getListaPrecios() + "' ");
                        oBCPick.executeQuery(false);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    BC.writeRecord();
                    BC.release();
                    BO.release();

                    System.out.println("Se crea registro de Lista de Precios por Cliente: " + sIndus.getTipoCuenta());
                    String mensaje = ("Se crea registro de Lista de Precios por Cliente: " + sIndus.getTipoCuenta());

                    String MsgSalida = "Creado Lista de Precios por Cliente";
                    this.CargaBitacoraSalidaCreado(sIndus.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(mensaje);

                }

            } catch (SiebelException e) {
                String error = e.getErrorMessage();
                String error2 = e.getDetailedMessage();
                System.out.println("Error en creacion Lista de Precios por Cliente:  " + sIndus.getTipoCuenta() + "     , con el mensaje:   " + error2.replace("'", " "));
                String MsgSalida = "Error al Crear Lista de Precios por Cliente";
                String FlagCarga = "E";
                String MsgError = "Error en creacion Lista de Precios por Cliente:  " + sIndus.getTipoCuenta() + "     , con el mensaje:   " + error2.replace("'", " ");

                this.CargaBitacoraSalidaError(sIndus.getRowId(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);
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
    public List<AdministracionListaPreciosCliente> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.X_ACCOUNT_TYPE \"Tipo Cuenta\", A.X_ACCOUNT_SUBTYPE \"Sub Tipo Cta\", A.X_DIGITAL_AREA \"Zona Dig\", A.X_PRICE_LST_NAME \"Lista Precios\"\n"
                + "FROM SIEBEL.CX_PRI_LST_ACCT A, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";

        List<AdministracionListaPreciosCliente> administracionListaPrecios = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {
            System.out.println("Creando Lista de registros a procesar.");
            while (rs.next()) {
                AdministracionListaPreciosCliente lista = new AdministracionListaPreciosCliente();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setTipoCuenta(rs.getString("Tipo Cuenta"));
                lista.setSubtipoCuenta(rs.getString("Sub Tipo Cta"));
                lista.setZonaDijitalizada(rs.getString("Zona Dig"));
                lista.setListaPrecios(rs.getString("Lista Precios"));
                administracionListaPrecios.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Tipo Cuenta"), rs.getString("Sub Tipo Cta"), rs.getString("Lista Precios"), "SE OBTIENEN DATOS", "LISTA DE PRECIOS POR CLIENTE",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = administracionListaPrecios.size();
            Boolean conteo = administracionListaPrecios.isEmpty(); // Valida si la lista esta vacia
            String mensaje;
            if (conteo) {
                System.out.println("No se obtuvieron Lista de Precios por Cliente o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron Lista de Precios por Cliente o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene Lista de Precios por Cliente, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Industrias, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return administracionListaPrecios;
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
