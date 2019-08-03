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
import interfaces.DAOPlazoForzoso;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import objetos.PlazoForzoso;

/**
 *
 * @author hector.pineda
 */
public class DAOCatalogoProductoPlazoForzosoImpl extends ConexionDB implements DAOPlazoForzoso {

    @Override
    public void inserta(List<PlazoForzoso> plazoForzoso, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);
        
         Boolean conteopadre = plazoForzoso.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

        for (PlazoForzoso sAdmin : plazoForzoso) {

            if ("SI".equals(Conectar)) {
                conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                Conectar = "NO";
            }

            try {

                String MsgError = "";

                SiebelBusObject BO = m_dataBean.getBusObject("TT Plazo Forzoso BO");
                SiebelBusComp BC = BO.getBusComp("TT Control Producto - Plazo BC");
                SiebelBusComp oBCPick = new SiebelBusComp();
                BC.activateField("TT Producto");
                BC.activateField("TT Plazo Forzoso");
                BC.clearToQuery();
                BC.setSearchExpr("[TT Producto] ='" + sAdmin.getProducto()+ "' ");
                BC.executeQuery(true);
                boolean reg = BC.firstRecord();
                if (reg) {

                    if (sAdmin.getPlazo() != null) {
                        if (!BC.getFieldValue("TT Plazo Forzoso").equals(sAdmin.getPlazo())) {
                            oBCPick = BC.getPicklistBusComp("TT Plazo Forzoso");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value] ='" + sAdmin.getPlazo()+ "' ");
                            oBCPick.executeQuery(false);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }
                    }
                    BC.writeRecord();
                    
                    System.out.println("Se valida existencia y/o actualizacion Catalogo Producto Forzoso:  " + sAdmin.getProducto());
                    String MsgSalida = "Se valida existencia y/o actualizacion de Catalogo Producto Forzoso";

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgSalida);

                    this.CargaBitacoraSalidaValidado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);
                    
                     BC.release();
                    BO.release();

                } else {
                    BC.newRecord(0);
                    
                    String Expresion = "[Name] ='" + sAdmin.getProducto()+ "'";
                    
                    if (sAdmin.getProducto() != null) {
                        oBCPick = BC.getPicklistBusComp("TT Producto");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Name] ='" + sAdmin.getProducto()+ "'");
                        oBCPick.executeQuery(false);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdmin.getPlazo() != null) {
                        oBCPick = BC.getPicklistBusComp("TT Plazo Forzoso");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Value] ='" + sAdmin.getPlazo()+ "'");
                        oBCPick.executeQuery(false);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    BC.writeRecord();

                    System.out.println("Se crea registro de Catalogo Producto Forzoso: " + sAdmin.getProducto());
                    String mensaje = ("Se crea registro de Catalogo Producto Forzoso: " + sAdmin.getProducto());

                    String MsgSalida = "Creado Catalogo Producto Forzoso";
                    this.CargaBitacoraSalidaCreado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(mensaje);

                    BC.release();
                    BO.release();

                }
            } catch (SiebelException e) {
                String error = e.getErrorMessage();
                System.out.println("Error en creacion Catalogo Producto Forzoso:  " + sAdmin.getProducto() + "     , con el mensaje:   " + error.replace("'", " "));
                String MsgSalida = "Error al Crear Catalogo Producto Forzoso";
                String FlagCarga = "E";
                String MsgError = "Error en creacion Catalogo Producto Forzoso:  " + sAdmin.getProducto() + "     , con el mensaje:   " + error.replace("'", " ");

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
    public List<PlazoForzoso> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, B.NAME \"Producto\", A.TT_PLAZO_FOR \"Plazo\"\n"
                + "FROM SIEBEL.CX_TT_ADMON_PF A , SIEBEL.S_PROD_INT B, SIEBEL.S_USER H\n"
                + "WHERE A.TT_PROD_ID = B.ROW_ID AND H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<PlazoForzoso> plazoForzoso = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros a procesar.");

            while (rs.next()) {
                PlazoForzoso lista = new PlazoForzoso();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setProducto(rs.getString("Producto"));
                lista.setPlazo(rs.getString("Plazo"));
                //lista.setFamilia(rs.getString("COMBO"));

                plazoForzoso.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Producto"), rs.getString("Plazo"), "SE OBTIENEN DATOS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = plazoForzoso.size();
            Boolean conteo = plazoForzoso.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Cat. Producto Plazo Forzoso o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Cat. Producto Plazo Forzoso o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Cat. Producto Plazo Forzoso, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Cat. Producto Plazo Forzoso, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return plazoForzoso;
    }

    private void CargaBitacoraEntrada(String Row_Id, String Val1, String Val2, String Seguimiento,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String TipoObjeto = Val1 + " : " + Val2;

        String searchRecordSQL1 = "SELECT ROW_ID FROM SIEBEL.CT_BITACORA WHERE ROW_ID = '" + Row_Id + "' AND USUARIO = '" + usuario + "' AND VERSION = '" + version + "' AND EXTRAER = '" + ambienteExtra + "' AND INSERTAR = '" + ambienteInser + "'";

        try (PreparedStatement ps = conexion.prepareStatement(searchRecordSQL1); ResultSet rs1 = ps.executeQuery()) {
            if (rs1.next()) {
                // sin accion
            } else {
                String setRecordSQL2 = "INSERT INTO SIEBEL.CT_BITACORA  (ROW_ID, TIPO_CATALOGO, TIPO_OBJETO, FLAG_CARGA, SEGUIMIENTO, MENSAJE_ERROR,USUARIO, VERSION, EXTRAER, INSERTAR)\n"
                        + "VALUES('" + Row_Id + "','CAT. PRODUCTO PLAZO FORZOSO','" + TipoObjeto + "','N','" + Seguimiento + "','','" + usuario + "','" + version + "','" + ambienteExtra + "','" + ambienteInser + "')";

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
