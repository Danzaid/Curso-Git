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
import interfaces.DAOAdministracionRazonesSociales;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import objetos.AdministracionRazonesSociales;

/**
 *
 * @author hector.pineda
 */
public class DAOAdministracionRazonesSocialesImpl extends ConexionDB implements DAOAdministracionRazonesSociales {

    @Override
    public void inserta(List<AdministracionRazonesSociales> administracionRazonesSociales, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);
        
         Boolean conteopadre = administracionRazonesSociales.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

        for (AdministracionRazonesSociales sAdmin : administracionRazonesSociales) {
            if ("SI".equals(Conectar)) {
                conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                Conectar = "NO";
            }

            try {

                String MsgError = "";

                SiebelBusObject BO = m_dataBean.getBusObject("Account");
                SiebelBusComp BC = BO.getBusComp("TT Admin RPT RS");
                SiebelBusComp oBCPick = new SiebelBusComp();
                BC.activateField("TT Codigo RPT");
                BC.activateField("TT Direccion");
                BC.activateField("TT IDO");
                BC.activateField("TT Razon Social");
                BC.activateField("TT RFC");
                BC.activateField("TT RPT");
                BC.activateField("TT SIC Code");
                BC.activateField("TT Telefono");
                BC.activateField("TT Compañia");
                BC.activateField("TT Reporte SAP Flag");
                BC.activateField("TT Telefono SWAT");
                BC.activateField("TT Telefono Neg SWAT");
                BC.clearToQuery();
                BC.setSearchExpr("[TT Codigo RPT] ='" + sAdmin.getCodigoRTP()+ "'");
                BC.executeQuery(true);
                boolean reg = BC.firstRecord();
                if (reg) {
                    if (sAdmin.getDireccionRazonSocial() != null) {
                        if (!BC.getFieldValue("TT Direccion").equals(sAdmin.getDireccionRazonSocial())) {
                            BC.setFieldValue("TT Direccion", sAdmin.getDireccionRazonSocial());
                        }
                    }
                    if (sAdmin.getIdoPortabilidad() != null) {
                        if (!BC.getFieldValue("TT IDO").equals(sAdmin.getIdoPortabilidad())) {
                            BC.setFieldValue("TT IDO", sAdmin.getIdoPortabilidad());
                        }
                    }
                    if (sAdmin.getRazonSocial() != null) {
                        if (!BC.getFieldValue("TT Razon Social").equals(sAdmin.getRazonSocial())) {
                            oBCPick = BC.getPicklistBusComp("TT Razon Social");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name] ='" + sAdmin.getRazonSocial()+ "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }
                    }
                    if (sAdmin.getRfcRazonSocil() != null) {
                        if (!BC.getFieldValue("TT RFC").equals(sAdmin.getRfcRazonSocil())) {
                            BC.setFieldValue("TT RFC", sAdmin.getRfcRazonSocil());
                        }
                    }
                    if (sAdmin.getRtp() != null) {
                        if (!BC.getFieldValue("TT RPT").equals(sAdmin.getRtp())) {
                            BC.setFieldValue("TT RPT", sAdmin.getRtp());
                        }
                    }
                    if (sAdmin.getSicCode() != null) {
                        if (!BC.getFieldValue("TT SIC Code").equals(sAdmin.getSicCode())) {
                            BC.setFieldValue("TT SIC Code", sAdmin.getSicCode());
                        }
                    }
                    if (sAdmin.getTelefono() != null) {
                        if (!BC.getFieldValue("TT Telefono").equals(sAdmin.getTelefono())) {
                            BC.setFieldValue("TT Telefono", sAdmin.getTelefono());
                        }
                    }
                    if (sAdmin.getCompañia() != null) {
                        if (!BC.getFieldValue("TT Compañia").equals(sAdmin.getCompañia())) {
                            BC.setFieldValue("TT Compañia", sAdmin.getCompañia());
                        }
                    }
                    if (sAdmin.getReporteSap() != null) {
                        if (!BC.getFieldValue("TT Reporte SAP Flag").equals(sAdmin.getReporteSap())) {
                            BC.setFieldValue("TT Reporte SAP Flag", sAdmin.getReporteSap());
                        }
                    }
                    if (sAdmin.getTelefonoSwatTeam() != null) {
                        if (!BC.getFieldValue("TT Telefono SWAT").equals(sAdmin.getTelefonoSwatTeam())) {
                            BC.setFieldValue("TT Telefono SWAT", sAdmin.getTelefonoSwatTeam());
                        }
                    }
                    if (sAdmin.getTelefonoSwatTeamNegocios() != null) {
                        if (!BC.getFieldValue("TT Telefono Neg SWAT").equals(sAdmin.getTelefonoSwatTeamNegocios())) {
                            BC.setFieldValue("TT Telefono Neg SWAT", sAdmin.getTelefonoSwatTeamNegocios());
                        }
                    }

                    BC.writeRecord();

                    BC.release();
                    BO.release();

                    System.out.println("Se valida existencia y/o actualizacion de Administracion de Razones Sociales:  " + sAdmin.getRazonSocial());
                    String MsgSalida = "Se valida existencia y/o actualizacion de  Administracion de Razones Sociales";

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgSalida);

                    this.CargaBitacoraSalidaValidado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                } else {
                    BC.newRecord(0);

                    if (sAdmin.getDireccionRazonSocial() != null) {
                        oBCPick = BC.getPicklistBusComp("TT Codigo RPT");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Name]='" + sAdmin.getCodigoRTP() + "' ");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdmin.getDireccionRazonSocial() != null) {
                        BC.setFieldValue("TT Direccion", sAdmin.getDireccionRazonSocial());
                    }

                    if (sAdmin.getIdoPortabilidad() != null) {
                        BC.setFieldValue("TT IDO", sAdmin.getIdoPortabilidad());
                    }

                    if (sAdmin.getRazonSocial() != null) {
                        oBCPick = BC.getPicklistBusComp("TT Razon Social");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Name]='" + sAdmin.getRazonSocial() + "' ");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdmin.getRfcRazonSocil() != null) {
                        BC.setFieldValue("TT RFC", sAdmin.getRfcRazonSocil());
                    }

//                    if (sAdmin.getIdoPortabilidad()!= null)
//                    BC.setFieldValue("TT RPT", sAdmin.getRtp());
//                    if (sAdmin.getSicCode()!= null)
//                    BC.setFieldValue("TT SIC Code", sAdmin.getSicCode());
                    if (sAdmin.getTelefono() != null) {
                        BC.setFieldValue("TT Telefono", sAdmin.getTelefono());
                    }

                    if (sAdmin.getCompañia() != null) {
                        BC.setFieldValue("TT Compañia", sAdmin.getCompañia());
                    }

                    if (sAdmin.getReporteSap() != null) {
                        BC.setFieldValue("TT Reporte SAP Flag", sAdmin.getReporteSap());
                    }

                    if (sAdmin.getTelefonoSwatTeam() != null) {
                        BC.setFieldValue("TT Telefono SWAT", sAdmin.getTelefonoSwatTeam());
                    }

                    if (sAdmin.getTelefonoSwatTeamNegocios() != null) {
                        BC.setFieldValue("TT Telefono Neg SWAT", sAdmin.getTelefonoSwatTeamNegocios());
                    }

                    BC.writeRecord();

                    System.out.println("Se crea registro de Administracion de Razones Sociales: " + sAdmin.getRazonSocial());
                    String mensaje = ("Se crea registro de Administracion de Razones Sociales: " + sAdmin.getRazonSocial());

                    String MsgSalida = "Creado Admon. de Razones Sociales";
                    this.CargaBitacoraSalidaCreado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(mensaje);

                    BC.release();
                    BO.release();

                }

            } catch (SiebelException e) {
                String error = e.getErrorMessage();
                System.out.println("Error en creacion Administracion de Razones Sociales:  " + sAdmin.getRazonSocial() + "     , con el mensaje:   " + error.replace("'", " "));
                String MsgSalida = "Error al Crear Administracion de Razones Sociales";
                String FlagCarga = "E";
                String MsgError = "Error en creacion Admon. de Razones Sociales:  " + sAdmin.getRazonSocial() + "     , con el mensaje:   " + error.replace("'", " ");

                this.CargaBitacoraSalidaError(sAdmin.getRowId(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);
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
    public List<AdministracionRazonesSociales> consultaPadre(String fechaI, String fechaT, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.CODIGO_RPT \"Codigo RPT\", A.DIRECCION_RS \"Direccion Razon Social\", A.IDO_PORT \"IDO de Portabilidad\", A.RAZON_SOCIAL \"Razon Social\", A.RFC_RS \"RFC de Razon Social\", A.RPT \"RPT\", \n"
                + "A.RS_SIC_CODE \"SIC Code\", A.TELEFONO_RS \"Telefono\", A.COMPANIA \"Compañia\", A.REPORTE_SAP_FLG \"Reporta a SAP\", A.TT_TELEFONO_SWAT \"Telefono SWAT Team\", \n"
                + "A.TT_TELEFONO_NEG_SWAT \"Telefono SWAT Team Negocios\"\n"
                + "FROM SIEBEL.CX_RPT_RAZONES A, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<AdministracionRazonesSociales> administracionRazonesSociales = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros a procesar.");

            while (rs.next()) {
                AdministracionRazonesSociales lista = new AdministracionRazonesSociales();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setCodigoRTP(rs.getString("Codigo RPT"));
                lista.setDireccionRazonSocial(rs.getString("Direccion Razon Social"));
                lista.setIdoPortabilidad(rs.getString("IDO de Portabilidad"));
                lista.setRazonSocial(rs.getString("Razon Social"));
                lista.setRfcRazonSocil(rs.getString("RFC de Razon Social"));
                lista.setRtp(rs.getString("RPT"));
                lista.setSicCode(rs.getString("SIC Code"));
                lista.setTelefono(rs.getString("Telefono"));
                lista.setCompañia(rs.getString("Compañia"));
                lista.setReporteSap(rs.getString("Reporta a SAP"));
                lista.setTelefonoSwatTeam(rs.getString("Telefono SWAT Team"));
                lista.setTelefonoSwatTeamNegocios(rs.getString("Telefono SWAT Team Negocios"));
                administracionRazonesSociales.add(lista);

                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Codigo RPT"), rs.getString("IDO de Portabilidad"), rs.getString("Razon Social"), "SE OBTIENEN DATOS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = administracionRazonesSociales.size();
            Boolean conteo = administracionRazonesSociales.isEmpty(); // Valida si la lista esta vacia
            String mensaje;
            if (conteo) {
                System.out.println("No se obtuvieron registros de Admon Razones Sociales o estas ya fueron procesados.");
                mensaje = "No se obtuvieron registros de Admon Razones Sociales o estas ya fueron procesados.";
            } else {
                System.out.println("Se obtiene lista de Admon Razones Sociales, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene lista de Admon Razones Sociales, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el mensaje:  " + ex);
            throw ex;
        }
        return administracionRazonesSociales;
    }

    private void CargaBitacoraEntrada(String Row_Id, String Valor0, String Valor1, String Valor2, String Seguimiento,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String TipoObjeto = Valor0 + " : " + Valor1 + " : " + Valor2;

        String searchRecordSQL1 = "SELECT ROW_ID FROM SIEBEL.CT_BITACORA WHERE ROW_ID = '" + Row_Id + "' AND USUARIO = '" + usuario + "' AND VERSION = '" + version + "' AND EXTRAER = '" + ambienteExtra + "' AND INSERTAR = '" + ambienteInser + "'";

        try (PreparedStatement ps = conexion.prepareStatement(searchRecordSQL1); ResultSet rs1 = ps.executeQuery()) {
            if (rs1.next()) {
                // sin accion
            } else {
                String setRecordSQL2 = "INSERT INTO SIEBEL.CT_BITACORA  (ROW_ID, TIPO_CATALOGO, TIPO_OBJETO, FLAG_CARGA, SEGUIMIENTO, MENSAJE_ERROR,USUARIO, VERSION, EXTRAER, INSERTAR)\n"
                        + "VALUES('" + Row_Id + "','ADMON. RAZONES SOCIALES','" + TipoObjeto + "','N','" + Seguimiento + "','','" + usuario + "','" + version + "','" + ambienteExtra + "','" + ambienteInser + "')";

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
