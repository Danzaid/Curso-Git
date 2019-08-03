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
import interfaces.DAOPuntosInventario;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import objetos.PuntosInventario;

/**
 *
 * @author hector.pineda
 */
public class DAOPuntosInventarioImpl extends ConexionDB implements DAOPuntosInventario {

    @Override
    public void inserta(List<PuntosInventario> puntosInventario, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);
        
         Boolean conteopadre = puntosInventario.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);
            
        for (PuntosInventario sAdmin : puntosInventario) {
            if ("SI".equals(Conectar)) {
                conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                Conectar = "NO";
            }
            try {

                String MsgError = "";

                SiebelBusObject BO = m_dataBean.getBusObject("FS Inventory Location Mgmt");
                SiebelBusComp BC = BO.getBusComp("FS Inventory Location");
//                SiebelBusComp BC = BO.getBusComp("FS Inventory Location Lookup");
                SiebelBusComp oBCPick = new SiebelBusComp();
                BC.activateField("Inventory Name");
                BC.activateField("Inventory Type");
                BC.activateField("Location Description");
                BC.activateField("TT RPT Ciudad");
                BC.activateField("Ownership");
                BC.activateField("CV_Password");
                BC.activateField("TT RPT");
                BC.activateField("TT RPT Id");
                BC.activateField("TT Division Financiera");
                BC.activateField("TT Compañia");
                BC.activateField("Organization");
                BC.clearToQuery();
                BC.setSearchExpr("[Inventory Name]='" + sAdmin.getNombre() + "' AND [Inventory Type]='" + sAdmin.getTipo() + "'");
                BC.executeQuery(true);
                boolean reg = BC.firstRecord();
                if (reg) {   
//                    if (sAdmin.getTipo() != null) {
//                        if (!BC.getFieldValue("Inventory Type").equals(sAdmin.getTipo())) {
//                            oBCPick = BC.getPicklistBusComp("Inventory Type");
//                            oBCPick.clearToQuery();
//                            oBCPick.setSearchExpr("[Inventory Location Type]='" + sAdmin.getTipo() + "' ");
//                            oBCPick.executeQuery(false);
//                            if (oBCPick.firstRecord()) {
//                                oBCPick.pick();
//                                BC.writeRecord();
//                            }
//                            oBCPick.release();
//                        }
//
//                    }
                    if (sAdmin.getDescripcion() != null) {
                        if (!BC.getFieldValue("Location Description").equals(sAdmin.getDescripcion())) {
                            BC.setFieldValue("Location Description", sAdmin.getDescripcion());
                            BC.writeRecord();
                        }
                    }

                    if (sAdmin.getCiudad() != null) {
                        if (!BC.getFieldValue("TT RPT Ciudad").equals(sAdmin.getCiudad())) {
                            oBCPick = BC.getPicklistBusComp("TT RPT Ciudad");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[TT RPT]='" + sAdmin.getCiudad() + "' ");
                            oBCPick.executeQuery(false);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();

                            }
                            oBCPick.release();
                        }
                    }

                    if (sAdmin.getPropiedad() != null) {
                        if (!BC.getFieldValue("Ownership").equals(sAdmin.getPropiedad())) {
                            oBCPick = BC.getPicklistBusComp("Ownership");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value]='" + sAdmin.getPropiedad() + "' ");
                            oBCPick.executeQuery(false);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();

                            }
                            oBCPick.release();
                        }
                    }

                    if (sAdmin.getPassword() != null) {
                        if (!BC.getFieldValue("CV_Password").equals(sAdmin.getPassword())) {
                            BC.setFieldValue("CV_Password", sAdmin.getPassword());

                        }
                    }

                    if (sAdmin.getCodigoRpt() != null) {
                        if (!BC.getFieldValue("TT RPT").equals(sAdmin.getCodigoRpt())) {
                            BC.setFieldValue("TT RPT", sAdmin.getCodigoRpt());

                        }
                    }

                    if (sAdmin.getRptId() != null) {
                        if (!BC.getFieldValue("TT RPT Id").equals(sAdmin.getRptId())) {
                            BC.setFieldValue("TT RPT Id", sAdmin.getRptId());

                        }
                    }

                    if (sAdmin.getDivisionFinanciera() != null) {
                        if (!BC.getFieldValue("TT Division Financiera").equals(sAdmin.getDivisionFinanciera())) {
                            BC.setFieldValue("TT Division Financiera", sAdmin.getDivisionFinanciera());

                        }
                    }

                    if (sAdmin.getCompañia() != null) {
                        if (!BC.getFieldValue("TT Compañia").equals(sAdmin.getCompañia())) {
                            BC.setFieldValue("TT Compañia", sAdmin.getCompañia());

                        }
                    }
                    BC.writeRecord();

                    System.out.println("Se valida existencia y/o actualizacion Todos los Puntos de Inventario:  " + sAdmin.getNombre());
                    String MsgSalida = "Se valida existencia y/o actualizacion de Todos los Puntos de Inventario";
                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgSalida);

                    this.CargaBitacoraSalidaValidado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                    BC.release();
                    BO.release();

                } else {
                    BC.newRecord(0);

                    if (sAdmin.getNombre() != null) {
                        BC.setFieldValue("Inventory Name", sAdmin.getNombre());
                    }

                    if (sAdmin.getTipo() != null) {
                        oBCPick = BC.getPicklistBusComp("Inventory Type");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Inventory Location Type]='" + sAdmin.getTipo() + "' ");
                        oBCPick.executeQuery(false);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }
                    if (sAdmin.getDescripcion() != null) {
                        BC.setFieldValue("Location Description", sAdmin.getDescripcion());
                    }

                    if (sAdmin.getCiudad() != null) {
                        oBCPick = BC.getPicklistBusComp("TT RPT Ciudad");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[TT RPT]='" + sAdmin.getCiudad() + "' ");
                        oBCPick.executeQuery(false);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }

                        oBCPick.release();;
                    }
                    if (sAdmin.getPropiedad() != null) {
                        oBCPick = BC.getPicklistBusComp("Ownership");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Value]='" + sAdmin.getPropiedad() + "' ");
                        oBCPick.executeQuery(false);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }
                    if (sAdmin.getPassword() != null) {
                        BC.setFieldValue("CV_Password", sAdmin.getPassword());
                    }

                    if (sAdmin.getCodigoRpt() != null) {
                        BC.setFieldValue("TT RPT", sAdmin.getCodigoRpt());
                    }

                    if (sAdmin.getRptId() != null) {
                        BC.setFieldValue("TT RPT Id", sAdmin.getRptId());
                    }

                    if (sAdmin.getDivisionFinanciera() != null) {
                        BC.setFieldValue("TT Division Financiera", sAdmin.getDivisionFinanciera());
                    }

                    if (sAdmin.getCompañia() != null) {
                        BC.setFieldValue("TT Compañia", sAdmin.getCompañia());
                    }

                    BC.writeRecord();
                    BC.release();
                    BO.release();

                    System.out.println("Se crea registro de Todos los Puntos de Inventario: " + sAdmin.getNombre());
                    String mensaje = ("Se crea registro de Todos los Puntos de Inventario: " + sAdmin.getNombre());

                    String MsgSalida = "Creado Industrias";
                    this.CargaBitacoraSalidaCreado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(mensaje);
                }

            } catch (SiebelException e) {
                String error = e.getErrorMessage();
                System.out.println("Error en creacion Todos los Puntos de Inventario:  " + sAdmin.getNombre() + "     , con el mensaje:   " + error.replace("'", " "));
                String MsgSalida = "Error al Crear Todos los Puntos de Inventario";
                String FlagCarga = "E";
                String MsgError = "Error en creacion Todos los Puntos de Inventario:  " + sAdmin.getNombre() + "     , con el mensaje:   " + error.replace("'", " ");

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
    public List<PuntosInventario> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.NAME \"Nombre\", B.NAME \"Tipo\", A.DESC_TEXT \"Descripción\", C.RPT \"Ciudad\", A.INV_OWNER_CD \"Propiedad\", A.X_PASSWORD \"Password\", C.CODIGO_RPT \"Código RPT\", A.X_TT_RPT \"RPT Id\",\n"
                + "A.X_TT_DIV_FIN \"División Financiera\", C.COMPANIA \"Compañía\", A.BU_ID \"Organización MVL\", 'N' AS FLAG_CARGA, NULL AS DESCRIPCION\n"
                + "FROM SIEBEL.S_INVLOC A, SIEBEL.S_INVLOC_TYPE B, SIEBEL.CX_RPT_RAZONES C, SIEBEL.S_USER H\n"
                + "WHERE A.INVLOC_TYPE_ID = B.ROW_ID AND A.X_TT_RPT = C.ROW_ID(+) AND H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA E WHERE E.ROW_ID = A.ROW_ID AND E.USUARIO ='" + usuario + "' AND E.VERSION ='" + version + "' AND E.EXTRAER ='" + ambienteExtra + "' AND E.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<PuntosInventario> puntosInventario = new LinkedList();

        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {
            System.out.println("Creando Lista de registros a procesar.");
            while (rs.next()) {
                PuntosInventario lista = new PuntosInventario();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setTipo(rs.getString("Tipo"));
                lista.setDescripcion(rs.getString("Descripción"));
                lista.setCiudad(rs.getString("Ciudad"));
                lista.setPropiedad(rs.getString("Propiedad"));
                lista.setPassword(rs.getString("Password"));
                lista.setCodigoRpt(rs.getString("Código RPT"));
                lista.setRptId(rs.getString("RPT Id"));
                lista.setDivisionFinanciera(rs.getString("División Financiera"));
                lista.setCompañia(rs.getString("Compañía"));
                lista.setOrganizacion(rs.getString("Organización MVL"));
                puntosInventario.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre"), rs.getString("Tipo"), "", "SE OBTIENEN DATOS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = puntosInventario.size();
            Boolean conteo = puntosInventario.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Todos los Puntos de Inventario o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Todos los Puntos de Inventario o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Todos los Puntos de Inventario, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Todos los Puntos de Inventario, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return puntosInventario;
    }

    private void CargaBitacoraEntrada(String Row_Id, String Val1, String Val2, String Val3, String Seguimiento, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String TipoObjeto = Val1 + " : " + Val2 + " : " + Val3;

       String searchRecordSQL1 = "SELECT ROW_ID FROM SIEBEL.CT_BITACORA WHERE ROW_ID = '" + Row_Id + "' AND USUARIO = '" + usuario + "' AND VERSION = '" + version + "' AND EXTRAER = '" + ambienteExtra + "' AND INSERTAR = '" + ambienteInser + "'";

        try (PreparedStatement ps = conexion.prepareStatement(searchRecordSQL1); ResultSet rs1 = ps.executeQuery()) {
            if (rs1.next()) {
                // sin accion
            } else {
                String setRecordSQL2 = "INSERT INTO SIEBEL.CT_BITACORA  (ROW_ID, TIPO_CATALOGO, TIPO_OBJETO, FLAG_CARGA, SEGUIMIENTO, MENSAJE_ERROR,USUARIO, VERSION, EXTRAER, INSERTAR)\n"
                        + "VALUES('" + Row_Id + "','TODOS LOS PUNTOS DE INVENTARIO','" + TipoObjeto + "','N','" + Seguimiento + "','','" + usuario + "','" + version + "','" + ambienteExtra + "','" + ambienteInser + "')";

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
