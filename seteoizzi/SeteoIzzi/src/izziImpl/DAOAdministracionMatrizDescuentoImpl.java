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
import interfaces.DAOAdministracionMatrizDescuentos;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import objetos.AdministracionMatrizDescuentos;

/**
 *
 * @author hector.pineda
 */
public class DAOAdministracionMatrizDescuentoImpl extends ConexionDB implements DAOAdministracionMatrizDescuentos {//Modificado

    @Override
    public void inserta(List<AdministracionMatrizDescuentos> administracionMatrizDescuentos, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);
        
         Boolean conteopadre = administracionMatrizDescuentos.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

        for (AdministracionMatrizDescuentos sAdminC : administracionMatrizDescuentos) {
            if ("SI".equals(Conectar)) {
                conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                Conectar = "NO";
            }

            try {

                String MsgError = "";

                SiebelBusObject BO = m_dataBean.getBusObject("CV Price Discount Matrix");
                SiebelBusComp BC = BO.getBusComp("CV Price Discount Matrix");
                SiebelBusComp oBCPick = new SiebelBusComp();
                BC.activateField("TV Prod Name");
                BC.activateField("Internet Prod Name");
                BC.activateField("Telefonia Prod Name");
                BC.activateField("Account Type");
                BC.activateField("Account Sub Type");
                BC.activateField("TV Discount %");
                BC.activateField("TV Discount % TC");
                BC.activateField("Internet Discount %");
                BC.activateField("Internet Discount % TC");
                BC.activateField("Telefonia Discount %");
                BC.activateField("Telefonia Discount % TC");
                BC.activateField("Combo Name");
                BC.activateField("Action Code");
                BC.activateField("CV Precio Com Efec");
                BC.activateField("CV Precio Com TDC");
                BC.activateField("Descripcion");
                BC.activateField("Tienda Plazo Credito");
                BC.activateField("Tienda Enganche");
                BC.clearToQuery();
                BC.setSearchExpr("[TV Prod Name] ='" + sAdminC.getProductoTv() + "' AND [Internet Prod Name] = '" + sAdminC.getProductoInternet() + "' "
                        + "AND [Telefonia Prod Name] = '" + sAdminC.getProductoTelefonía() + "' AND [Account Type] = '" + sAdminC.getTipoCuenta() + "' "
                        + "AND [Account Sub Type] = '" + sAdminC.getSubtipoCuenta() + "' AND [Combo Name] = '" + sAdminC.getNombreCombo() + "'");
//                        + "AND [Tienda Enganche]='" + sAdminC.getTiendaenganche() + "' AND [Tienda Plazo Credito]='" + sAdminC.getTiendaplazocred() + "'");
                BC.executeQuery(true);
                boolean reg = BC.firstRecord();
                if (reg) {
//                    if (sAdminC.getTipoCuenta() != null) {
//                        if (!BC.getFieldValue("Account Type").equals(sAdminC.getTipoCuenta())) {
//                            BC.setFieldValue("Account Type", sAdminC.getTipoCuenta());
//                        }
//                    }
//                    if (sAdminC.getCodigoAccion() != null) {
//                        if (!BC.getFieldValue("Account Sub Type").equals(sAdminC.getSubtipoCuenta())) {
//                            BC.setFieldValue("Account Sub Type", sAdminC.getSubtipoCuenta());
//                        }
//                    }
                    if (sAdminC.getDescuentoTv() != null) {
                        if (!BC.getFieldValue("TV Discount %").equals(sAdminC.getDescuentoTv())) {
                            BC.setFieldValue("TV Discount %", sAdminC.getDescuentoTv());
                        }
                    }
                    if (sAdminC.getDescuentoTvTarjetaCrédito() != null) {
                        if (!BC.getFieldValue("TV Discount %").equals(sAdminC.getDescuentoTvTarjetaCrédito())) {
                            BC.setFieldValue("TV Discount % TC", sAdminC.getDescuentoTvTarjetaCrédito());
                        }
                    }
                    if (sAdminC.getDescuentoInternet() != null) {
                        if (!BC.getFieldValue("Internet Discount %").equals(sAdminC.getDescuentoInternet())) {
                            BC.setFieldValue("Internet Discount %", sAdminC.getDescuentoInternet());
                        }
                    }
                    if (sAdminC.getDescuentoInternetTarjetaCrédito() != null) {
                        if (!BC.getFieldValue("Internet Discount % TC").equals(sAdminC.getDescuentoInternetTarjetaCrédito())) {
                            BC.setFieldValue("Internet Discount % TC", sAdminC.getDescuentoInternetTarjetaCrédito());
                        }
                    }
                    if (sAdminC.getDescuentoTelefonía() != null) {
                        if (!BC.getFieldValue("Telefonia Discount %").equals(sAdminC.getDescuentoTelefonía())) {
                            BC.setFieldValue("Telefonia Discount %", sAdminC.getDescuentoTelefonía());
                        }
                    }
                    if (sAdminC.getDescuentoTelefoníaTarjetaCrédito() != null) {
                        if (!BC.getFieldValue("Telefonia Discount % TC").equals(sAdminC.getDescuentoTelefoníaTarjetaCrédito())) {
                            BC.setFieldValue("Telefonia Discount % TC", sAdminC.getDescuentoTelefoníaTarjetaCrédito());
                        }
                    }
//                    if (sAdminC.getNombreCombo() != null) {
//                        if (!BC.getFieldValue("Combo Name").equals(sAdminC.getNombreCombo())) {
//                            BC.setFieldValue("Combo Name", sAdminC.getNombreCombo());
//                        }
//                    }
                    if (sAdminC.getCodigoAccion() != null) {
                        if (!BC.getFieldValue("Action Code").equals(sAdminC.getCodigoAccion())) {
                            BC.setFieldValue("Action Code", sAdminC.getCodigoAccion());
                        }
                    }
                    if (sAdminC.getPrecioEfectivo() != null) {
                        if (!BC.getFieldValue("CV Precio Com Efec").equals(sAdminC.getPrecioEfectivo())) {
                            BC.setFieldValue("CV Precio Com Efec", sAdminC.getPrecioEfectivo());
                        }
                    }
                    if (sAdminC.getPrecioTarjetaCrédito() != null) {
                        if (!BC.getFieldValue("CV Precio Com TDC").equals(sAdminC.getPrecioTarjetaCrédito())) {
                            BC.setFieldValue("CV Precio Com TDC", sAdminC.getPrecioTarjetaCrédito());
                        }
                    }
                    if (sAdminC.getDescripción() != null) {
                        if (!BC.getFieldValue("Descripcion").equals(sAdminC.getDescripción())) {
                            BC.setFieldValue("Descripcion", sAdminC.getDescripción());
                        }
                    }
                    BC.writeRecord();

                    BC.release();
                    BO.release();

                    System.out.println("Se valida existencia y/o actualizacion de Admon Matriz de Descuento:  " + sAdminC.getProductoTv());
                    String MsgSalida = "Se valida existencia y/o actualizacion de Admon Matriz de Descuento";
                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgSalida);

                    this.CargaBitacoraSalidaValidado(sAdminC.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);
                } else {
                    BC.newRecord(0);

                    if (sAdminC.getProductoTv() != null) {
                        oBCPick = BC.getPicklistBusComp("TV Prod Name");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Name]='" + sAdminC.getProductoTv() + "' ");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdminC.getProductoInternet() != null) {
                        oBCPick = BC.getPicklistBusComp("Internet Prod Name");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Name]='" + sAdminC.getProductoInternet() + "' ");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdminC.getProductoTelefonía() != null) {
                        oBCPick = BC.getPicklistBusComp("Telefonia Prod Name");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Name]='" + sAdminC.getProductoTelefonía() + "' ");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }
                    
                    if (sAdminC.getTipoCuenta() != null) {
                        oBCPick = BC.getPicklistBusComp("Account Type");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Value]='" + sAdminC.getTipoCuenta() + "' ");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }
                    
                    if (sAdminC.getSubtipoCuenta() != null) {
                        oBCPick = BC.getPicklistBusComp("Account Sub Type");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Parent]='" + sAdminC.getTipoCuenta() + "' AND [Value]='" + sAdminC.getSubtipoCuenta() + "'");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }


                    if (sAdminC.getDescuentoTv() != null) {
                        BC.setFieldValue("TV Discount %", sAdminC.getDescuentoTv());
                    }

                    if (sAdminC.getDescuentoTvTarjetaCrédito() != null) {
                        BC.setFieldValue("TV Discount % TC", sAdminC.getDescuentoTvTarjetaCrédito());
                    }

                    if (sAdminC.getDescuentoInternet() != null) {
                        BC.setFieldValue("Internet Discount %", sAdminC.getDescuentoInternet());
                    }

                    if (sAdminC.getDescuentoInternetTarjetaCrédito() != null) {
                        BC.setFieldValue("Internet Discount % TC", sAdminC.getDescuentoInternetTarjetaCrédito());
                    }

                    if (sAdminC.getDescuentoTelefonía() != null) {
                        BC.setFieldValue("Telefonia Discount %", sAdminC.getDescuentoTelefonía());
                    }

                    if (sAdminC.getDescuentoTelefoníaTarjetaCrédito() != null) {
                        BC.setFieldValue("Telefonia Discount % TC", sAdminC.getDescuentoTelefoníaTarjetaCrédito());
                    }

                    if (sAdminC.getNombreCombo() != null) {
                        BC.setFieldValue("Combo Name", sAdminC.getNombreCombo());
                    }

                    if (sAdminC.getCodigoAccion() != null) {
                        BC.setFieldValue("Action Code", sAdminC.getCodigoAccion());
                    }

                    if (sAdminC.getPrecioEfectivo() != null) {
                        BC.setFieldValue("CV Precio Com Efec", sAdminC.getPrecioEfectivo());
                    }

                    if (sAdminC.getPrecioTarjetaCrédito() != null) {
                        BC.setFieldValue("CV Precio Com TDC", sAdminC.getPrecioTarjetaCrédito());
                    }

                    if (sAdminC.getDescripción() != null) {
                        BC.setFieldValue("Descripcion", sAdminC.getDescripción());
                    }
                    
                    if (sAdminC.getTiendaenganche() != null) {
                        BC.setFieldValue("Tienda Enganche",String.valueOf(sAdminC.getTiendaenganche()) );
                    }else{
                        BC.setFieldValue("Tienda Enganche", Integer.toString(0) );
                    }
                    
                    if (sAdminC.getTiendaplazocred()!= null) {
                        BC.setFieldValue("Tienda Plazo Credito", sAdminC.getTiendaplazocred());
                    }else{
                        BC.setFieldValue("Tienda Plazo Credito", Integer.toString(0) );
                    }

                    BC.writeRecord();

                    BC.release();
                    BO.release();

                    System.out.println("Se crea registro de Admon Matriz de Descuento: " + sAdminC.getProductoTv());
                    String MsgSalida = "Creado Admon Matriz de Descuento";
                    this.CargaBitacoraSalidaCreado(sAdminC.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                    String mensaje = ("Se crea registro de Admon Matriz de Descuento: " + sAdminC.getProductoTv());
                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(mensaje);

                }

            } catch (SiebelException e) {
                String error = e.getErrorMessage();
                String error2 = e.getDetailedMessage();
                System.out.println("Error en creacion Admon Matriz de Descuento:  " + sAdminC.getProductoTv() + "     , con el mensaje:   " + error.replace("'", " "));
                String MsgSalida = "Error al Crear  Admon Matriz de Descuento";
                String FlagCarga = "E";
                String MsgError = "Error en creacion  Admon Matriz de Descuento:  " + sAdminC.getProductoTv() + "     , con el mensaje:   " + error.replace("'", " ");

                this.CargaBitacoraSalidaError(sAdminC.getRowId(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);
                Reportes rep = new Reportes();
                rep.agregarTextoAlfinal(MsgError);
            } finally {
                Contador++;
//                    System.out.println("Contador =  " + Contador);

                if (Contador == Maxima) {
                    conSiebel.CloseSiebel(m_dataBean);
                    Contador = 0;
                    Conectar = "SI";
//                        System.out.println("Se inicia contador a =  " + Contador);
                }
            }
        }
        }
    }

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
    public List<AdministracionMatrizDescuentos> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, B.NAME \"Producto de TV\", C.NAME \"Producto de Internet\", D.NAME \"Producto de Telefonía\",A.ACCOUNT_TYPE \"Tipo de Cuenta\", A.ACCNT_SUB_TYPE \"Subtipo de Cuenta\", \n"
                + "A.TV_DISC_PER \"% Descuento TV\", A.TV_DISC_PER_TC \"% Descuento TV con TC\", A.INT_DISC_PER \"% Descuento Internet\", A.INT_DISC_PER_TC \"% Descuento Internet con TC\", \n"
                + "A.TEL_DISC_PER \"% Descuento Telefonía\", A.TEL_DISC_PER_TC \"% Descuento Telefonía con TC\", A.COMBO_NAME \"Nombre del Combo\", A.ACTION_CD \"Codigo de Accion\", \n"
                + "A.PRECIO_COM_EFE \"Precio Efectivo\", A.PRECIO_COM_TDC \"Precio Tarjeta de Crédito\", A.DESCRIPTION \"Descripción\", A.TIENDA_PLAZO_CRED \"Tienda Plazo Cred\",A.TIENDA_ENGANCHE \"Tienda Enganche\"\n"
                + "FROM SIEBEL.CX_PRI_DISC_MTX A, SIEBEL.S_PROD_INT B, SIEBEL.S_PROD_INT C, SIEBEL.S_PROD_INT D, SIEBEL.S_USER H\n"
                + "WHERE A.TV_PROD_ID = B.ROW_ID AND A.INT_PROD_ID = C.ROW_ID AND A.TEL_PROD_ID = D.ROW_ID AND H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA E WHERE E.ROW_ID = A.ROW_ID AND E.USUARIO ='" + usuario + "' AND E.VERSION ='" + version + "' AND E.EXTRAER ='" + ambienteExtra + "' AND E.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<AdministracionMatrizDescuentos> administracionMatrizDescuentos = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros a procesar.");

            while (rs.next()) {
                AdministracionMatrizDescuentos lista = new AdministracionMatrizDescuentos();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setProductoTv(rs.getString("Producto de TV"));
                lista.setProductoInternet(rs.getString("Producto de Internet"));
                lista.setProductoTelefonía(rs.getString("Producto de Telefonía"));
                lista.setTipoCuenta(rs.getString("Tipo de Cuenta"));
                lista.setSubtipoCuenta(rs.getString("Subtipo de Cuenta"));
                lista.setDescuentoTv(rs.getString("% Descuento TV"));
                lista.setDescuentoTvTarjetaCrédito(rs.getString("% Descuento TV con TC"));
                lista.setDescuentoInternet(rs.getString("% Descuento Internet"));
                lista.setDescuentoInternetTarjetaCrédito(rs.getString("% Descuento Internet con TC"));
                lista.setDescuentoTelefonía(rs.getString("% Descuento Telefonía"));
                lista.setDescuentoTelefoníaTarjetaCrédito(rs.getString("% Descuento Telefonía con TC"));
                lista.setNombreCombo(rs.getString("Nombre del Combo"));
                lista.setCodigoAccion(rs.getString("Codigo de Accion"));
                lista.setPrecioEfectivo(rs.getString("Precio Efectivo"));
                lista.setPrecioTarjetaCrédito(rs.getString("Precio Tarjeta de Crédito"));
                lista.setDescripción(rs.getString("Descripción"));
                lista.setTiendaenganche(rs.getString("Tienda Enganche"));
                lista.setTiendaplazocred(rs.getString("Tienda Plazo Cred"));

                administracionMatrizDescuentos.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Producto de TV"), rs.getString("Producto de Internet"), rs.getString("Producto de Telefonía"), "SE OBTIENEN DATOS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = administracionMatrizDescuentos.size();
            Boolean conteo = administracionMatrizDescuentos.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron registros de Admon Matriz de Descuentos o estas ya fueron procesados.");
                mensaje = "No se obtuvieron registros de Admon Matriz de Descuentos o estas ya fueron procesados.";
            } else {
                System.out.println("Se obtiene Admon Matriz de Descuentos, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene Admon Matriz de Descuentos, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el mensaje:  " + ex);
            throw ex;
        }
        return administracionMatrizDescuentos;
    }

    private void CargaBitacoraEntrada(String Row_Id, String Val1, String Val2, String Val3, String Val4, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String TipoObjeto = Val1 + " : " + Val2 + " : " + Val3;

        String searchRecordSQL1 = "SELECT ROW_ID FROM SIEBEL.CT_BITACORA WHERE ROW_ID = '" + Row_Id + "' AND USUARIO = '" + usuario + "' AND VERSION = '" + version + "' AND EXTRAER = '" + ambienteExtra + "' AND INSERTAR = '" + ambienteInser + "'";

        try (PreparedStatement ps = conexion.prepareStatement(searchRecordSQL1); ResultSet rs1 = ps.executeQuery()) {
            if (rs1.next()) {
                // sin accion
            } else {
                String setRecordSQL2 = "INSERT INTO SIEBEL.CT_BITACORA  (ROW_ID, TIPO_CATALOGO, TIPO_OBJETO, FLAG_CARGA, SEGUIMIENTO, MENSAJE_ERROR,USUARIO, VERSION, EXTRAER, INSERTAR)\n"
                        + "VALUES('" + Row_Id + "','ADMON. MATRIZ DE DESCUENTOS','" + TipoObjeto + "','N','" + Val4 + "','','" + usuario + "','" + version + "','" + ambienteExtra + "','" + ambienteInser + "')";

                PreparedStatement ps1 = conexion.prepareStatement(setRecordSQL2);
                ps1.executeUpdate();
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
