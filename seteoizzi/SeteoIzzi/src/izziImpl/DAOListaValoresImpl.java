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
import interfaces.DAOListaValores;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import objetos.ListaValores;
import objetos.ListaValoresPadre;

/**
 *
 * @author hector.pineda
 */
public class DAOListaValoresImpl extends ConexionDB implements DAOListaValores {

    @Override
    public void inserta(List<ListaValores> listaValores, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {


        String LovType = "";
        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);
        
         Boolean conteopadre = listaValores.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

        for (ListaValores sAdmin : listaValores) {

            if ("SI".equals(Conectar)) {
                conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                Conectar = "NO";
            }

            try {

                String MsgError = "";
                String Ejecutar = "SI";

                SiebelBusObject BO = m_dataBean.getBusObject("List Of Values");
                SiebelBusComp BC = BO.getBusComp("List Of Values");
                SiebelBusComp oBCPick = new SiebelBusComp();
                String IdPadre = null;
                
                // Obtiene el ID del Parent en el ambiente destino
                if (sAdmin.getPadreJerarquia() != null) {
                    IdPadre = this.ObtieneIdJerarquia(BC, sAdmin.getTipopadre(), sAdmin.getValorpadre(), sAdmin.getCodigonodepidiompadre(), sAdmin.getSubtipo());
                    if (IdPadre == null) {
                        System.out.println("No se encontro Id de correspondencia de Jerarquia en el ambiente destino, la tipificacion de jerarquia padre que se valido es: " + sAdmin.getTipopadre() + " : " + sAdmin.getValorpadre() + " : " + sAdmin.getCodigonodepidiompadre());
                        String MsgSalida = "Error al Crear LOV";
                        String FlagCarga = "E";
                        String MsgErr = "No se encontro Id de correspondencia de Jerarquia en el ambiente destino, la tipificacion de jerarquia padre que se valido es: " + sAdmin.getTipopadre() + " : " + sAdmin.getValorpadre() + " : " + sAdmin.getCodigonodepidiompadre();

                        this.CargaBitacoraSalidaError(sAdmin.getRowId(), MsgSalida, FlagCarga, MsgErr,usuario,version,ambienteInser,ambienteExtra);
                        Ejecutar = "NO";
                    }
                }
                //
                if ("SI".equals(Ejecutar)) {

                    BC.activateField("Type");
                    BC.activateField("Value");
                    BC.activateField("Name");
                    BC.activateField("Parent");
                    BC.activateField("High");
                    BC.activateField("Active");
                    BC.activateField("Low");
                    BC.activateField("Translate");
                    BC.activateField("Language Name");
                    BC.activateField("Order By");
                    BC.activateField("Description");
                    BC.activateField("Replication Level");
                    BC.activateField("Parent Id");
                    BC.activateField("Language");
                    BC.activateField("Sub Type");
                    BC.clearToQuery();

                    if (IdPadre != null) {
                        BC.setSearchExpr("[Type] ='" + sAdmin.getTipo() + "' AND [Value]= '" + sAdmin.getValorVisible() + "' AND [Name]='" + sAdmin.getCodigoNoDependeIdioma() + "' "
                                + "AND [Language] = '" + sAdmin.getNombreIdioma() + "' AND [Parent Id] = '" + IdPadre + "' AND [Sub Type] ='" + sAdmin.getSubtipo() + "'");
                    } else {
                        BC.setSearchExpr("[Type] ='" + sAdmin.getTipo() + "' AND [Value]= '" + sAdmin.getValorVisible() + "' AND [Name]='" + sAdmin.getCodigoNoDependeIdioma() + "'");
                    }

                    BC.executeQuery(true);
                    boolean reg = BC.firstRecord();
                    if (reg) {

                        String IdEncontrado = BC.getFieldValue("Id");  // PARA USAR EN DEBUG

                        if (sAdmin.getValorVisible() != null) {
                            if (!BC.getFieldValue("Value").equals(sAdmin.getValorVisible())) {
                                BC.setFieldValue("Value", sAdmin.getValorVisible());
                            }
                        }
                        if (sAdmin.getCodigoNoDependeIdioma() != null) {
                            if (!BC.getFieldValue("Name").equals(sAdmin.getCodigoNoDependeIdioma())) {
                                BC.setFieldValue("Name", sAdmin.getCodigoNoDependeIdioma());
                            }
                        }

                        if (sAdmin.getAlto() != null) {
                            if (!BC.getFieldValue("High").equals(sAdmin.getAlto())) {
                                BC.setFieldValue("High", sAdmin.getAlto());
                            }
                        }
                        if (sAdmin.getActivo() != null) {
                            if (!BC.getFieldValue("Active").equals(sAdmin.getActivo())) {
                                BC.setFieldValue("Active", sAdmin.getActivo());
                            }
                        }
                        if (sAdmin.getBajo() != null) {
                            if (!BC.getFieldValue("Low").equals(sAdmin.getBajo())) {
                                BC.setFieldValue("Low", sAdmin.getBajo());
                            }
                        }
                        if (sAdmin.getTraducir() != null) {
                            if (!BC.getFieldValue("Translate").equals(sAdmin.getTraducir())) {
                                BC.setFieldValue("Translate", sAdmin.getTraducir());
                            }
                        }

                        if (sAdmin.getNombreIdioma() != null) {
                            if (!BC.getFieldValue("Language").equals(sAdmin.getNombreIdioma())) {
                                oBCPick = BC.getPicklistBusComp("Language Name");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Language Code]='" + sAdmin.getIdIdioma() + "'");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }
                        if (sAdmin.getOrden() != null) {
                            if (!BC.getFieldValue("Order By").equals(sAdmin.getOrden())) {
                                BC.setFieldValue("Order By", sAdmin.getOrden());
                            }
                        }
                        if (sAdmin.getDescripcion() != null) {
                            if (!BC.getFieldValue("Description").equals(sAdmin.getDescripcion())) {
                                BC.setFieldValue("Description", sAdmin.getDescripcion());
                            }
                        }

                        if (sAdmin.getPadreJerarquia() != null) {

                            if (!BC.getFieldValue("Parent Id").equals(IdPadre)) {
                                oBCPick = BC.getPicklistBusComp("Parent");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Id]='" + IdPadre + "'");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        BC.writeRecord();

                        BC.release();
                        BO.release();

                        System.out.println("Se valida existencia y/o actualizacion LOV:  " + sAdmin.getTipo() + " : " + sAdmin.getValorVisible() + " : " + sAdmin.getCodigoNoDependeIdioma());
                        String MsgSalida = "Se valida existencia y/o actualizacion de LOV";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                    } else {

                        if (!LovType.equals(sAdmin.getTipo())) { // Para validar o crear solo 1 vez el LOV_TYPE
                            this.cargaBC(BC, oBCPick, sAdmin.getTipo(),usuario,version,ambienteInser,ambienteExtra);  // Valida creacion de padres LOV_TYPES
                            LovType = sAdmin.getTipo();
                        }

                        BC.newRecord(0);

                        if (sAdmin.getTipo() != null) {
                            oBCPick = BC.getPicklistBusComp("Type");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value]='" + sAdmin.getTipo() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdmin.getValorVisible() != null) {
                            BC.setFieldValue("Value", sAdmin.getValorVisible());
                        }

                        if (sAdmin.getCodigoNoDependeIdioma() != null) {
                            BC.setFieldValue("Name", sAdmin.getCodigoNoDependeIdioma());
                        }

                        if (sAdmin.getAlto() != null) {
                            BC.setFieldValue("High", sAdmin.getAlto());
                        }

                        if (sAdmin.getActivo() != null) {
                            BC.setFieldValue("Active", sAdmin.getActivo());
                        }

                        if (sAdmin.getBajo() != null) {
                            BC.setFieldValue("Low", sAdmin.getBajo());
                        }

                        if (sAdmin.getTraducir() != null) {
                            BC.setFieldValue("Translate", sAdmin.getTraducir());
                        }

                        if (sAdmin.getIdIdioma() != null) {
                            oBCPick = BC.getPicklistBusComp("Language Name");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Language Code]='" + sAdmin.getIdIdioma() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdmin.getOrden() != null) {
                            BC.setFieldValue("Order By", sAdmin.getOrden());
                        }

                        if (sAdmin.getDescripcion() != null) {
                            BC.setFieldValue("Description", sAdmin.getDescripcion());
                        }

                        if (sAdmin.getPadreJerarquia() != null) {
                            oBCPick = BC.getPicklistBusComp("Parent");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Id]='" + IdPadre + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        BC.writeRecord();

                        String IdCreado = BC.getFieldValue("Id"); // PARA USAR EN DEBUG

                        BC.release();
                        BO.release();

                        System.out.println("Se crea registro de LOV: " + sAdmin.getTipo() + " : " + sAdmin.getValorVisible() + " : " + sAdmin.getCodigoNoDependeIdioma());
                        String mensaje = ("Se crea registro de LOV: " + sAdmin.getTipo() + " : " + sAdmin.getValorVisible() + " : " + sAdmin.getCodigoNoDependeIdioma());

                        String MsgSalida = "Creado LOV";
                        this.CargaBitacoraSalidaCreado(sAdmin.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

                    }
                }
            } catch (SiebelException e) {

                String errorsys = e.getErrorMessage();
                System.out.println("Error en creacion LOV:  " + sAdmin.getTipo() + " : " + sAdmin.getValorVisible() + " : " + sAdmin.getCodigoNoDependeIdioma() + "     , con el mensaje:   " + errorsys.replace("'", " "));
                String MsgSalida = "Error al Crear LOV";
                String FlagCarga = "E";
                String MsgError = "Error en creacion LOV:  " + sAdmin.getTipo() + " : " + sAdmin.getValorVisible() + " : " + sAdmin.getCodigoNoDependeIdioma() + "     , con el mensaje:   " + errorsys.replace("'", " ");

                this.CargaBitacoraSalidaError(sAdmin.getRowId(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

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
    public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick, String listValores,String usuario,String version,String ambienteInser,String ambienteExtra) throws SiebelException {
        try {
            List<ListaValoresPadre> combo = new LinkedList();
            combo = this.consultaLisValores(listValores,usuario,version,ambienteInser,ambienteExtra);
            for (ListaValoresPadre sAdminC : combo) {
                try {
                    String MsgSalida = "";
                    String MsgError = "";

                    BC.activateField("Type");
                    BC.activateField("Value");
                    BC.activateField("Name");
                    BC.activateField("Parent");
                    BC.activateField("High");
                    BC.activateField("Active");
                    BC.activateField("Low");
                    BC.activateField("Translate");
                    BC.activateField("Language Name");
                    BC.activateField("Order By");
                    BC.activateField("Description");
                    BC.activateField("Replication Level");
                    BC.clearToQuery();
                    BC.setSearchExpr("[Type] ='" + sAdminC.getTipo() + "' AND [Value]= '" + sAdminC.getValorVisible() + "' "
                            + "AND [Name]='" + sAdminC.getCodigoNoDependeIdioma() + "'");
                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {
                        // SIN ACCION
                        System.out.println("Se valida existencia de LOV_TYPE:  " + sAdminC.getValorVisible());
                        MsgSalida = "Se valida existencia de LOV_TYPE";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                    } else {

                        BC.newRecord(0);

                        if (sAdminC.getTipo() != null) {
                            oBCPick = BC.getPicklistBusComp("Type");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value] = 'LOV_TYPE'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getValorVisible() != null) {
                            BC.setFieldValue("Value", sAdminC.getValorVisible());
                        }

                        if (sAdminC.getCodigoNoDependeIdioma() != null) {
                            BC.setFieldValue("Name", sAdminC.getCodigoNoDependeIdioma());
                        }

                        if (sAdminC.getTraducir() != null) {
                            BC.setFieldValue("Translate", sAdminC.getTraducir());
                        }

                        if (sAdminC.getDescripcion() != null) {
                            BC.setFieldValue("Description", sAdminC.getDescripcion());
                        }

                        BC.writeRecord();

                        System.out.println("Se crea padre LOV_TYPE:  " + sAdminC.getValorVisible() + "  , con el ID:  " + BC.getFieldValue("Id"));
                        MsgSalida = "Creado LOV_TYPE";

                        this.CargaBitacoraSalidaCreado(sAdminC.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);
                    }
                } catch (Exception e) {
                    String errorsys = e.getMessage();
                    System.out.println("Error en creacion LOV_TYPE:  " + sAdminC.getValorVisible() + "     , con el mensaje:   " + errorsys.replace("'", " "));
                    String MsgSalida = "Error al Crear LOV_TYPE";
                    String FlagCarga = "E";
                    String MsgError = "Error en creacion LOV_TYPE:  " + sAdminC.getValorVisible() + "     , con el mensaje:   " + errorsys.replace("'", " ");

                    this.CargaBitacoraSalidaError(sAdminC.getRowId(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgSalida);
                }
            }
        } catch (Exception ex) {
            System.out.println("Error al obtener el pade LOV_TYPE, con el mensaje: " + ex);
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
    public List<ListaValoresPadre> consultaLisValores(String listValores,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.TYPE \"Tipo\", A.VAL \"Valor visible\", A.NAME \"Cód que no depende del idioma\", A.PAR_ROW_ID \"Id Padre Jerarquia\", B.NAME \"Cód indepen del idioma Princi\", A.HIGH \"Alto\", \n"
                + "A.ACTIVE_FLG \"Activo\", A.LOW \"Bajo\", A.TRANSLATE_FLG \"Traducir\",A.LANG_ID \"Codigo Idioma\", C.NAME \"Nombre del idioma\", A.ORDER_BY \"Orden\", A.DESC_TEXT \"Descripción\", A.RPLCTN_LVL_CD \"Nivel de replicación\"\n"
                + "FROM SIEBEL.S_LST_OF_VAL A, SIEBEL. S_LST_OF_VAL B, SIEBEL.S_LANG C\n"
                + "WHERE A.PAR_ROW_ID = B.ROW_ID(+) AND A.LANG_ID = C.LANG_CD\n"
                + "AND A.TYPE = 'LOV_TYPE' AND A.VAL = '" + listValores + "' \n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID)\n"
                + "ORDER BY A.CREATED ASC";
        List<ListaValoresPadre> listaValores = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                ListaValoresPadre lista = new ListaValoresPadre();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setTipo(rs.getString("Tipo"));
                lista.setValorVisible(rs.getString("Valor visible"));
                lista.setCodigoNoDependeIdioma(rs.getString("Cód que no depende del idioma"));
                lista.setPadreJerarquia(rs.getString("Id Padre Jerarquia"));
                lista.setCodigoIndependienteIdioma(rs.getString("Cód indepen del idioma Princi"));
                lista.setAlto(rs.getString("Alto"));
                lista.setActivo(rs.getString("Activo"));
                lista.setBajo(rs.getString("Bajo"));
                lista.setTraducir(rs.getString("Traducir"));
                lista.setIdIdioma(rs.getString("Codigo Idioma"));
                lista.setNombreIdioma(rs.getString("Nombre del idioma"));
                lista.setOrden(rs.getString("Orden"));
                lista.setDescripcion(rs.getString("Descripción"));
                lista.setNivelReplicacion(rs.getString("Nivel de replicación"));
                listaValores.add(lista);

                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Tipo"), rs.getString("Valor visible"), rs.getString("Cód que no depende del idioma"), "SE OBTIENEN DATOS",usuario,version,ambienteInser,ambienteExtra);
            }
//            int Registros = listaValores.size();
            Boolean conteo = listaValores.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvo LOV_TYPE para procesar o ya fue procesada.");
                mensaje = "No se obtuvo LOV_TYPE para procesar o ya fue procesada.";
            } else {
                System.out.println("Se obtiene LOV_TYPE.");
                mensaje = "Se obtiene LOV_TYPE.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en consultaLisValores, con el error:  " + ex);
//            throw ex;
        } finally {

        }
        return listaValores;
    }


    @Override
    public List<ListaValores> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.TYPE \"Tipo\", A.VAL \"Valor visible\", A.NAME \"Cód que no depende del idioma\", A.PAR_ROW_ID \"Id Padre Jerarquia\", B.NAME \"Cód indepen del idioma Princi\", B.SUB_TYPE \"Sub Tipo\", E.TYPE \"Tipo Padre\", E.VAL \"Valor Padre\", E.NAME \"Cod no dep idiom Padre\",\n"
                + "A.HIGH \"Alto\", A.ACTIVE_FLG \"Activo\", A.LOW \"Bajo\", A.TRANSLATE_FLG \"Traducir\",A.LANG_ID \"Codigo Idioma\", C.NAME \"Nombre del idioma\", A.ORDER_BY \"Orden\", A.DESC_TEXT\"Descripción\", A.RPLCTN_LVL_CD \"Nivel de replicación\"\n"
                + "FROM SIEBEL.S_LST_OF_VAL A, SIEBEL.S_LST_OF_VAL B, SIEBEL.S_LANG C, SIEBEL.S_LST_OF_VAL E, SIEBEL.S_USER H\n"
                + "WHERE A.PAR_ROW_ID = B.ROW_ID(+) AND A.LANG_ID = C.LANG_CD AND A.PAR_ROW_ID = E.ROW_ID(+) AND H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.TYPE <> 'LOV_TYPE'\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";

        List<ListaValores> listaValores = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros a procesar.");

            while (rs.next()) {
                ListaValores lista = new ListaValores();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setTipo(rs.getString("Tipo"));
                lista.setValorVisible(rs.getString("Valor visible"));
                lista.setCodigoNoDependeIdioma(rs.getString("Cód que no depende del idioma"));
                lista.setPadreJerarquia(rs.getString("Id Padre Jerarquia"));
                lista.setCodigoIndependienteIdioma(rs.getString("Cód indepen del idioma Princi"));
                lista.setAlto(rs.getString("Alto"));
                lista.setActivo(rs.getString("Activo"));
                lista.setBajo(rs.getString("Bajo"));
                lista.setTraducir(rs.getString("Traducir"));
                lista.setIdIdioma(rs.getString("Codigo Idioma"));
                lista.setNombreIdioma(rs.getString("Nombre del idioma"));
                lista.setOrden(rs.getString("Orden"));
                lista.setDescripcion(rs.getString("Descripción"));
                lista.setNivelReplicacion(rs.getString("Nivel de replicación"));
                lista.setTipopadre(rs.getString("Tipo Padre"));
                lista.setValorpadre(rs.getString("Valor Padre"));
                lista.setCodigonodepidiompadre(rs.getString("Cod no dep idiom Padre"));
                lista.setSubtipo(rs.getString("Sub Tipo"));

                listaValores.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Tipo"), rs.getString("Valor visible"), rs.getString("Cód que no depende del idioma"), "SE OBTIENEN DATOS",usuario,version,ambienteInser,ambienteExtra);
            }
            int Registros = listaValores.size();
            Boolean conteo = listaValores.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de valores o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de valores o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de valores, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de valores, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            String Msge = ex.getMessage();
            System.out.println("Error en ConsultaPadre, con el error:  " + Msge.replace("'", " "));
//            throw ex;
        }
        return listaValores;
    }

    private void CargaBitacoraEntrada(String Row_Id, String Val1, String Val2, String Val3, String Seguimiento,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String TipoObjeto = Val1 + " : " + Val2 + " : " + Val3;

        String searchRecordSQL1 = "SELECT ROW_ID FROM SIEBEL.CT_BITACORA WHERE ROW_ID = '" + Row_Id + "' AND USUARIO = '" + usuario + "' AND VERSION = '" + version + "' AND EXTRAER = '" + ambienteExtra + "' AND INSERTAR = '" + ambienteInser + "'";

        try (PreparedStatement ps = conexion.prepareStatement(searchRecordSQL1); ResultSet rs1 = ps.executeQuery()) {
            if (rs1.next()) {
                // sin accion
            } else {
                String setRecordSQL2 = "INSERT INTO SIEBEL.CT_BITACORA  (ROW_ID, TIPO_CATALOGO, TIPO_OBJETO, FLAG_CARGA, SEGUIMIENTO, MENSAJE_ERROR,USUARIO, VERSION, EXTRAER, INSERTAR)\n"
                        + "VALUES('" + Row_Id + "','LISTA DE VALORES','" + TipoObjeto + "','N','" + Seguimiento + "','','" + usuario + "','" + version + "','" + ambienteExtra + "','" + ambienteInser + "')";

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
    
    
    private String ObtieneIdJerarquia(SiebelBusComp BC, String Tipo, String Valor, String CodIndIdioma, String SubTipo) throws SiebelException {

        String Regresa = null;
        try {
            BC.activateField("Type");
            BC.activateField("Value");
            BC.activateField("Name");
            BC.activateField("Sub Type");
            BC.clearToQuery();
            if (SubTipo != null) {
                BC.setSearchExpr("[Type] ='" + Tipo + "' AND [Value]= '" + Valor + "' AND [Name]='" + CodIndIdioma + "' AND [Sub Type]= '" + SubTipo + "'");
            } else {
                BC.setSearchExpr("[Type] ='" + Tipo + "' AND [Value]= '" + Valor + "' AND [Name]='" + CodIndIdioma + "'");
            }
            BC.executeQuery(true);
            boolean regchild = BC.firstRecord();
            if (regchild) {
                Regresa = BC.getFieldValue("Id");
            }

        } catch (Exception ex) {
            System.out.println("Error al obtener el pade LOV_TYPE, con el mensaje: " + ex);
        }
        return Regresa;
    }
}
