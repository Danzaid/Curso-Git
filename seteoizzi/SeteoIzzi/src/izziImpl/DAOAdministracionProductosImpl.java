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
import interfaces.DAOAdministracionProductos;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import objetos.AdministracionProductos;

/**
 *
 * @author hector.pineda
 */
public class DAOAdministracionProductosImpl extends ConexionDB implements DAOAdministracionProductos {

    @Override
    public void inserta(List<AdministracionProductos> administracionProducto, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw) throws Exception {

        ConexionSiebel conSiebel = new ConexionSiebel();
        SiebelDataBean m_dataBean = new SiebelDataBean();
        conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);

        for (AdministracionProductos sAdmin : administracionProducto) {

            if ("SI".equals(Conectar)) {
                conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                Conectar = "NO";
            }

            try {

                String MsgError = "";

                SiebelBusObject BO = m_dataBean.getBusObject("Admin ISS Product Definition");
                SiebelBusComp BC = BO.getBusComp("Internal Product - ISS Admin - FOR IMPORT Products");
                BC.activateField("Name");
                BC.activateField("Inclusive Eligibility Flag");
                BC.activateField("Pre Pick CD");
                BC.clearToQuery();
                BC.setSearchExpr("[Name]='" + sAdmin.getNombre() + "' ");
                BC.executeQuery(true);
                boolean reg = BC.firstRecord();
                if (reg) {
                    if (sAdmin.getElegibilidad() != null) {
                        if (!BC.getFieldValue("Inclusive Eligibility Flag").equals(sAdmin.getElegibilidad())) {
                            BC.setFieldValue("Inclusive Eligibility Flag", sAdmin.getElegibilidad());

                        }
                    }

                    if (sAdmin.getComprobarElegibilidad() != null) {
                        if (!BC.getFieldValue("Pre Pick CD").equals(sAdmin.getComprobarElegibilidad())) {
                            BC.setFieldValue("Pre Pick CD", sAdmin.getComprobarElegibilidad());
                        }
                    }
                    BC.writeRecord();

                    BC.release();
                    BO.release();

                    System.out.println("Se valida existencia y/o actualizacion de Administracion de Productos:  " + sAdmin.getNombre());
                    String MsgSalida = "Se valida existencia y/o actualizacion de Administracion de Productos";

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgSalida);

                    this.CargaBitacoraSalidaValidado(sAdmin.getRowId(), MsgSalida, "Y", MsgError);

                } else {
//                    BC.newRecord(0);
//
//                    if (sAdmin.getNombre() != null) {
//                        BC.setFieldValue("Name", sAdmin.getNombre());
//                    }
//
//                    if (sAdmin.getElegibilidad() != null) {
//                        BC.setFieldValue("Inclusive Eligibility Flag", sAdmin.getElegibilidad());
//                    }
//
//                    if (sAdmin.getComprobarElegibilidad() != null) {
//                        BC.setFieldValue("Pre Pick CD", sAdmin.getComprobarElegibilidad());
//                    }
//
//                    BC.writeRecord();
                    
                    
                    System.out.println("Este producto no se encontro para actualizar, debe crearse nuevo: " + sAdmin.getNombre());
                    String mensaje = ("Este producto no se encontro para actualizar, debe crearse nuevo: " + sAdmin.getNombre());
                    String MsgSalida = "Este producto no se encontro, debe crearse nuevo";

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(mensaje);

                    this.CargaBitacoraSalidaCreado(sAdmin.getRowId(), MsgSalida, "E", MsgError);
                    
                    BC.release();
                    BO.release();
                }
                

            } catch (SiebelException e) {
                String error = e.getErrorMessage();
                System.out.println("Error en proceso: Administracion de Productos:  " + sAdmin.getNombre() + "     , con el mensaje:   " + error.replace("'", " "));
                String MsgSalida = "Error en proceso: Administracion de Productos";
                String FlagCarga = "E";
                String MsgError = "Error en proceso: Administracion de Productos:  " + sAdmin.getNombre() + "     , con el mensaje:   " + error.replace("'", " ");

                this.CargaBitacoraSalidaError(sAdmin.getRowId(), MsgSalida, FlagCarga, MsgError);
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
        }conSiebel.CloseSiebel(m_dataBean);
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
    public List<AdministracionProductos> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.NAME \"Nombre de Producto\", A.INCLSV_ELIG_RL_FLG \"Elegibilidad inclusiva\", A.APPLY_EC_RULE_FLG \"Comprobar elegibilidad\"\n"
                + "FROM SIEBEL.S_PROD_INT A WHERE A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID)\n"
                + "ORDER BY A.CREATED ASC";
        List<AdministracionProductos> administracionProductos = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros a procesar.");

            while (rs.next()) {
                AdministracionProductos lista = new AdministracionProductos();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setNombre(rs.getString("Nombre de Producto"));
                lista.setElegibilidad(rs.getString("Elegibilidad inclusiva"));
                lista.setComprobarElegibilidad(rs.getString("Comprobar elegibilidad"));

                administracionProductos.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre de Producto"), rs.getString("Elegibilidad inclusiva"), rs.getString("Comprobar elegibilidad"), "SE OBTIENEN DATOS");

            }
            int Registros = administracionProductos.size();
            Boolean conteo = administracionProductos.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron Productos o estas ya fueron procesados.");
                mensaje = "No se obtuvieron Productos o estas ya fueron procesados.";
            } else {
                System.out.println("Se obtiene Productos, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene Productos, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return administracionProductos;
    }

    private void CargaBitacoraEntrada(String Row_Id, String Valor0, String Valor1, String Valor2, String Seguimiento) throws Exception {

        String TipoObjeto = Valor0 + " : " + Valor1 + " : " + Valor2;

        String searchRecordSQL1 = "SELECT ROW_ID FROM SIEBEL.CT_BITACORA WHERE ROW_ID = '" + Row_Id + "'";

        try (PreparedStatement ps = conexion.prepareStatement(searchRecordSQL1); ResultSet rs1 = ps.executeQuery()) {
            if (rs1.next()) {
                // sin accion
            } else {
                String setRecordSQL2 = "INSERT INTO SIEBEL.CT_BITACORA  (ROW_ID, TIPO_CATALOGO, TIPO_OBJETO, FLAG_CARGA, SEGUIMIENTO, MENSAJE_ERROR)\n"
                        + "VALUES('" + Row_Id + "','ADMINISTRACION DE PRODUCTOS','" + TipoObjeto + "','N','" + Seguimiento + "','')";

                PreparedStatement ps1 = conexion.prepareStatement(setRecordSQL2);
                ps1.executeUpdate();
                ps1.close();
            }
            rs1.close();
            ps.close();

        }
    }

    private void CargaBitacoraSalidaCreado(String rowId, String MsgSalida, String FlagCarga, String MsgError) throws Exception {

        String readRecordSQL4 = "UPDATE SIEBEL.CT_BITACORA SET FLAG_CARGA = '" + FlagCarga + "', SEGUIMIENTO = '" + MsgSalida + "', MENSAJE_ERROR = '" + MsgError + "'\n"
                + "WHERE ROW_ID = '" + rowId + "'";

        PreparedStatement ps3 = conexion.prepareStatement(readRecordSQL4);
        ps3.executeUpdate();
        ps3.close();
    }

    private void CargaBitacoraSalidaValidado(String rowId, String MsgSalida, String FlagCarga, String MsgError) throws Exception {

        String searchRecordSQL3 = "SELECT ROW_ID, FECHA, TIPO_CATALOGO, TIPO_OBJETO, FLAG_CARGA, SEGUIMIENTO, MENSAJE_ERROR,RELEASE FROM SIEBEL.CT_BITACORA \n"
                + "WHERE FLAG_CARGA= 'Y' AND SEGUIMIENTO LIKE '%Creado%' AND ROW_ID = '" + rowId + "'";

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

    private void CargaBitacoraSalidaError(String rowId, String MsgSalida, String FlagCarga, String MsgError) throws Exception {

        String readRecordSQL6 = "UPDATE SIEBEL.CT_BITACORA SET FLAG_CARGA = '" + FlagCarga + "', SEGUIMIENTO = '" + MsgSalida + "', MENSAJE_ERROR = '" + MsgError + "'\n"
                + "WHERE ROW_ID = '" + rowId + "'";

        PreparedStatement ps5 = conexion.prepareStatement(readRecordSQL6);
        ps5.executeUpdate();
        ps5.close();
    }

    private String validaejecucion(String ROWID) throws Exception {

        String searchRecordSQL1 = "SELECT ROW_ID FROM SIEBEL.CT_BITACORA WHERE ROW_ID = '" + ROWID + "' AND FLAG_CARGA = 'Y'";

        String Agregar = "";

        try (PreparedStatement ps8 = conexion.prepareStatement(searchRecordSQL1); ResultSet rs8 = ps8.executeQuery()) {
            if (rs8.next()) {
                Agregar = "NO";
            } else {
                Agregar = "SI";
            }
            rs8.close();
            ps8.close();

            return (Agregar);

        }

    }

}
