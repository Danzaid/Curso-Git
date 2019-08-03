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
import interfaces.DAOAltaPromociones;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import objetos.AltaPromociones;

/**
 *
 * @author hector.pineda
 */
public class DAOAltaPromocionesImpl extends ConexionDB implements DAOAltaPromociones {

    @Override
    public void inserta(List<AltaPromociones> altaPromociones, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);
        
         Boolean conteopadre = altaPromociones.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

        for (AltaPromociones sAdmin : altaPromociones) {
            if ("SI".equals(Conectar)) {
                conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                Conectar = "NO";
            }

            try {

                String MsgError = "";

                SiebelBusObject BO = m_dataBean.getBusObject("TT Registro Promociones");
                SiebelBusComp BC = BO.getBusComp("TT Alta Promociones V2");
                BC.activateField("Id Promocion");
                BC.activateField("Nombre Promocion");
                BC.activateField("Activa");
                BC.activateField("Descuento");
                BC.activateField("Producto Base");
                BC.activateField("Edo Base");
                BC.activateField("Producto Promocion");
                BC.activateField("Edo Promocion");
                BC.activateField("Producto Req");
                BC.activateField("Edo Requerido");
                BC.activateField("Familia");
                BC.activateField("Fecha Inicio");
                BC.activateField("Fecha Fin");
                BC.activateField("Meses");
                BC.activateField("Rtp");
                BC.activateField("Tipo Promo");
                BC.activateField("Segmento");
                BC.clearToQuery();
                BC.setSearchSpec("Id Promocion", sAdmin.getIdPromocion());
                BC.setSearchSpec("Nombre Promocion", sAdmin.getNombrePromocion());
                BC.executeQuery(true);
                boolean reg = BC.firstRecord();
                if (reg) {
                    if (sAdmin.getActiva() != null) {
                        if (!BC.getFieldValue("Activa").equals(sAdmin.getActiva())) {
                            BC.setFieldValue("Activa", sAdmin.getActiva());
                        }
                    }
                    if (sAdmin.getDescuento() != null) {
                        if (!BC.getFieldValue("Descuento").equals(sAdmin.getDescuento())) {
                            BC.setFieldValue("Descuento", sAdmin.getDescuento());
                        }
                    }
                    if (sAdmin.getProductoBase() != null) {
                        if (!BC.getFieldValue("Producto Base").equals(sAdmin.getProductoBase())) {
                            BC.setFieldValue("Producto Base", sAdmin.getProductoBase());
                        }
                    }
                    if (sAdmin.getEdoBase() != null) {
                        if (!BC.getFieldValue("Edo Base").equals(sAdmin.getEdoBase())) {
                            BC.setFieldValue("Edo Base", sAdmin.getEdoBase());
                        }
                    }
                    if (sAdmin.getProductoPromocion() != null) {
                        if (!BC.getFieldValue("Producto Promocion").equals(sAdmin.getProductoPromocion())) {
                            BC.setFieldValue("Producto Promocion", sAdmin.getProductoPromocion());
                        }
                    }
                    if (sAdmin.getEdoPromocion() != null) {
                        if (!BC.getFieldValue("Edo Promocion").equals(sAdmin.getEdoPromocion())) {
                            BC.setFieldValue("Edo Promocion", sAdmin.getEdoPromocion());
                        }
                    }
                    if (sAdmin.getProductoReq() != null) {
                        if (!BC.getFieldValue("Producto Req").equals(sAdmin.getProductoReq())) {
                            BC.setFieldValue("Producto Req", sAdmin.getProductoReq());
                        }
                    }
                    if (sAdmin.getEdoRequerido() != null) {
                        if (!BC.getFieldValue("Edo Requerido").equals(sAdmin.getEdoRequerido())) {
                            BC.setFieldValue("Edo Requerido", sAdmin.getEdoRequerido());
                        }
                    }
                    if (sAdmin.getFamilia() != null) {
                        if (!BC.getFieldValue("Familia").equals(sAdmin.getFamilia())) {
                            BC.setFieldValue("Familia", sAdmin.getFamilia());
                        }
                    }
                    if (sAdmin.getFechaInicio() != null) {
                        SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                        String dateString = format.format(sAdmin.getFechaInicio());
                        BC.setFieldValue("Fecha Inicio", dateString);
                    }
                    if (sAdmin.getFechaFin() != null) {
                        SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                        String datefin = format.format(sAdmin.getFechaFin());
                        BC.setFieldValue("Fecha Fin", datefin);
                    }
                    if (sAdmin.getMeses() != null) {
                        if (!BC.getFieldValue("Meses").equals(sAdmin.getMeses())) {
                            BC.setFieldValue("Meses", sAdmin.getMeses());
                        }
                    }
                    if (sAdmin.getrTP() != null) {
                        if (!BC.getFieldValue("Rtp").equals(sAdmin.getrTP())) {
                            BC.setFieldValue("Rtp", sAdmin.getrTP());
                        }
                    }
                    if (sAdmin.getTipoPromo() != null) {
                        if (!BC.getFieldValue("Tipo Promo").equals(sAdmin.getTipoPromo())) {
                            BC.setFieldValue("Tipo Promo", sAdmin.getTipoPromo());
                        }
                    }
                    if (sAdmin.getSegmento() != null) {
                        if (!BC.getFieldValue("Segmento").equals(sAdmin.getSegmento())) {
                            BC.setFieldValue("Segmento", sAdmin.getSegmento());
                        }
                    }
                    BC.writeRecord();

                    BC.release();
                    BO.release();

                    System.out.println("Se valida existencia y/o actualizacion Alta Promociones:  " + sAdmin.getNombrePromocion());
                    String MsgSalida = "Se valida existencia y/o actualizacion de Alta Promociones";

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgSalida);

                    this.CargaBitacoraSalidaValidado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);
                } else {
                    BC.newRecord(0);

                    if (sAdmin.getIdPromocion() != null) {
                        BC.setFieldValue("Id Promocion", sAdmin.getIdPromocion());
                    }

                    if (sAdmin.getNombrePromocion() != null) {
                        BC.setFieldValue("Nombre Promocion", sAdmin.getNombrePromocion());
                    }

                    if (sAdmin.getActiva() != null) {
                        BC.setFieldValue("Activa", sAdmin.getActiva());
                    }

                    if (sAdmin.getDescuento() != null) {
                        BC.setFieldValue("Descuento", sAdmin.getDescuento());
                    }

                    if (sAdmin.getProductoBase() != null) {
                        BC.setFieldValue("Producto Base", sAdmin.getProductoBase());
                    }

                    if (sAdmin.getEdoBase() != null) {
                        BC.setFieldValue("Edo Base", sAdmin.getEdoBase());
                    }

                    if (sAdmin.getProductoPromocion() != null) {
                        BC.setFieldValue("Producto Promocion", sAdmin.getProductoPromocion());
                    }

                    if (sAdmin.getEdoPromocion() != null) {
                        BC.setFieldValue("Edo Promocion", sAdmin.getEdoPromocion());
                    }

                    if (sAdmin.getProductoReq() != null) {
                        BC.setFieldValue("Producto Req", sAdmin.getProductoReq());
                    }

                    if (sAdmin.getEdoRequerido() != null) {
                        BC.setFieldValue("Edo Requerido", sAdmin.getEdoRequerido());
                    }

                    if (sAdmin.getFamilia() != null) {
                        BC.setFieldValue("Familia", sAdmin.getFamilia());
                    }

                    if (sAdmin.getFechaInicio() != null) {
                        SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                        String dateString = format.format(sAdmin.getFechaInicio());
                        BC.setFieldValue("Fecha Inicio", dateString);
                    }

                    if (sAdmin.getFechaFin() != null) {
                        SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                        String datefin = format.format(sAdmin.getFechaFin());
                        BC.setFieldValue("Fecha Fin", datefin);
                    }
                    if (sAdmin.getMeses() != null) {
                        BC.setFieldValue("Meses", sAdmin.getMeses());
                    }

                    if (sAdmin.getrTP() != null) {
                        BC.setFieldValue("Rtp", sAdmin.getrTP());
                    }

                    if (sAdmin.getTipoPromo() != null) {
                        BC.setFieldValue("Tipo Promo", sAdmin.getTipoPromo());
                    }

                    if (sAdmin.getSegmento() != null) {
                        BC.setFieldValue("Segmento", sAdmin.getSegmento());
                    }

                    BC.writeRecord();

                    BC.release();
                    BO.release();

                    System.out.println("Se crea registro de Alta Promociones: " + sAdmin.getNombrePromocion());
                    String mensaje = ("Se crea registro de Alta Promociones: " + sAdmin.getNombrePromocion());

                    String MsgSalida = "Creado Alta Promociones";
                    this.CargaBitacoraSalidaCreado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(mensaje);

                }
            } catch (SiebelException e) {
                String error = e.getErrorMessage();
                System.out.println("Error en creacion Alta Promociones:  " + sAdmin.getNombrePromocion() + "     , con el mensaje:   " + error.replace("'", " "));
                String MsgSalida = "Error al Crear Alta Promociones";
                String FlagCarga = "E";
                String MsgError = "Error en creacion Alta Promociones:  " + sAdmin.getNombrePromocion() + "     , con el mensaje:   " + error.replace("'", " ");

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
    public List<AltaPromociones> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.ID_PROMOCION \"Id Promocion\", A.NOMBRE_PROMOCION \"Nombre Promocion\", A.ACTIVA \"Activa\", A.DESCUENTO \"Descuento\", A.PRODUCTO_BASE \"Producto Base\", A.EDO_BASE \"Edo Base\", \n"
                + "A.PRODUCTO_PROMOCION \"Producto Promocion\", A.EDO_PROMOCION \"Edo Promocion\", A.PRODUCTO_REQ \"Producto Req\", A.EDO_REQUERIDO \"Edo Requerido\", A.FAMILIA \"Familia\", A.FECHA_INICIO \"Fecha Inicio\", \n"
                + "A.FECHA_FIN \"Fecha Fin\", A.MESES \"Meses\", A.RTP \"RTP\", A.TIPO_PROMO \"Tipo Promo\", A.SEGMENTO \"Segmento\"\n"
                + "FROM SIEBEL.CX_ALTA_PROM_V2 A, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<AltaPromociones> altaPromociones = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros a procesar.");

            while (rs.next()) {
                AltaPromociones lista = new AltaPromociones();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setIdPromocion(rs.getString("Id Promocion"));
                lista.setNombrePromocion(rs.getString("Nombre Promocion"));
                lista.setActiva(rs.getString("Activa"));
                lista.setDescuento(rs.getString("Descuento"));
                lista.setProductoBase(rs.getString("Producto Base"));
                lista.setEdoBase(rs.getString("Edo Base"));
                lista.setProductoPromocion(rs.getString("Producto Promocion"));
                lista.setEdoPromocion(rs.getString("Edo Promocion"));
                lista.setProductoReq(rs.getString("Producto Req"));
                lista.setEdoRequerido(rs.getString("Edo Requerido"));
                lista.setFamilia(rs.getString("Familia"));
                lista.setFechaInicio(rs.getDate("Fecha Inicio"));
                lista.setFechaFin(rs.getDate("Fecha Fin"));
                lista.setMeses(rs.getString("Meses"));
                lista.setrTP(rs.getString("RTP"));
                lista.setTipoPromo(rs.getString("Tipo Promo"));
                lista.setSegmento(rs.getString("Segmento"));

                altaPromociones.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Id Promocion"), rs.getString("Nombre Promocion"), "SE OBTIENEN DATOS",usuario,version,ambienteInser,ambienteExtra);
            }
            int Registros = altaPromociones.size();
            Boolean conteo = altaPromociones.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Alta Promociones o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Alta Promociones o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Alta Promociones, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Alta Promociones, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return altaPromociones;
    }

    private void CargaBitacoraEntrada(String Row_Id, String Val1, String Val2, String Seguimiento,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String TipoObjeto = Val1 + " : " + Val2;

        String searchRecordSQL1 = "SELECT ROW_ID FROM SIEBEL.CT_BITACORA WHERE ROW_ID = '" + Row_Id + "' AND USUARIO = '" + usuario + "' AND VERSION = '" + version + "' AND EXTRAER = '" + ambienteExtra + "' AND INSERTAR = '" + ambienteInser + "'";

        try (PreparedStatement ps = conexion.prepareStatement(searchRecordSQL1); ResultSet rs1 = ps.executeQuery()) {
            if (rs1.next()) {
                // sin accion
            } else {
                String setRecordSQL2 = "INSERT INTO SIEBEL.CT_BITACORA  (ROW_ID, TIPO_CATALOGO, TIPO_OBJETO, FLAG_CARGA, SEGUIMIENTO, MENSAJE_ERROR,USUARIO, VERSION, EXTRAER, INSERTAR)\n"
                        + "VALUES('" + Row_Id + "','ALTA PROMOCIONES','" + TipoObjeto + "','N','" + Seguimiento + "','','" + usuario + "','" + version + "','" + ambienteExtra + "','" + ambienteInser + "')";

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
