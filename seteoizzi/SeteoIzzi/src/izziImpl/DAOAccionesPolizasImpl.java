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
import interfaces.DAOAccionesPolizas;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import objetos.AccionesPolizasPadre;
import objetos.AccionesPolizasHijo;
import objetos.AccionesPolizasSoloHijos;

/**
 *
 * @author hector.pineda
 */
public class DAOAccionesPolizasImpl extends ConexionDB implements DAOAccionesPolizas {

    @Override
    public void inserta(List<AccionesPolizasPadre> accionesPolizas, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);

        Boolean conteopadre = accionesPolizas.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

            for (AccionesPolizasPadre sAdminC : accionesPolizas) {   // Realiza acciones si encuentra modificaciones en el Padre y despues profundica en los hijos
                if ("SI".equals(Conectar)) {
                    conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                    Conectar = "NO";
                }

                try {

                    String MsgError = "";

                    SiebelBusObject BO = m_dataBean.getBusObject("Workflow Action Type");
                    SiebelBusComp BC = BO.getBusComp("Workflow Action Type");
                    SiebelBusComp BCCHILD = BO.getBusComp("Workflow Action Argument");
                    SiebelBusComp oBCPick = new SiebelBusComp();

                    BC.activateField("Name");
                    BC.activateField("Program");
                    BC.activateField("Object");
                    BC.clearToQuery();
                    BC.setSearchExpr("[Name] ='" + sAdminC.getNombre() + "'");
                    BC.executeQuery(true);
                    boolean reg = BC.firstRecord();
                    if (reg) {

                        System.out.println("Se valida existencia y/o actualizacion Acciones  de Polizas:  " + sAdminC.getNombre());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Acciones de Polizas";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

//                        String RowId1 = BC.getFieldValue("Id");
//                        this.cargaBC(BCCHILD, oBCPick, RowId1, sAdminC.getRowId(),usuario,version,ambienteInser,ambienteExtra);

                        BCCHILD.release();
                        BC.release();
                        BO.release();

                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getNombre() != null) {
                            BC.setFieldValue("Name", sAdminC.getNombre());
                        }

                        if (sAdminC.getPrograma() != null) {
                            oBCPick = BC.getPicklistBusComp("Program");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name] ='" + sAdminC.getPrograma() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getObjetoFlujoTrabajo() != null) {
                            oBCPick = BC.getPicklistBusComp("Object");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name] ='" + sAdminC.getObjetoFlujoTrabajo() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        BC.writeRecord();

                        System.out.println("Se crea registro de Acciones  de  Polizas: " + sAdminC.getNombre());
                        String mensaje = ("Se crea registro de Acciones  de  Polizas: " + sAdminC.getNombre());

                        String MsgSalida = "Creado Acciones  de  Polizas";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

//                        String RowID = BC.getFieldValue("Id");
//                        this.cargaBC(BCCHILD, oBCPick, RowID, sAdminC.getRowId(),usuario,version,ambienteInser,ambienteExtra);

                        BCCHILD.release();
                        BC.release();
                        BO.release();

                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en creacion Acciones  de  Polizas:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Acciones  de  Polizas";
                    String FlagCarga = "E";
                    String MsgError = "Error en creacion Acciones  de  Polizas:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " ");

                    this.CargaBitacoraSalidaError(sAdminC.getRowId(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);
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

        try {                                                                       // BUSCA REGISTROS NUEVOS O ACTUALIZADOS, DESDE EL HIJO 
            List<AccionesPolizasSoloHijos> hijo = new LinkedList();
            hijo = this.consultasoloHijos(fechaIni, fechaTer,usuario,version,ambienteInser,ambienteExtra);

            Boolean conteosolohijo = hijo.isEmpty(); // VALIDA SI LA LISTA ESTA VACIA
            if (!conteosolohijo) {

                ConexionSiebel conSiebel = new ConexionSiebel();
                SiebelDataBean m_dataBean = new SiebelDataBean();
                conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

                String NombrePadre = "";
                String IdPadre = null;

                for (AccionesPolizasSoloHijos sAdminC : hijo) {  // SI LA LISTA NO ESTA VACIA, COMIENZA EJECUCION
                    if ("SI".equals(Conectar)) {
                        conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                        Conectar = "NO";
                    }
                    try {
                        SiebelBusObject BO = m_dataBean.getBusObject("Workflow Action Type");
                        SiebelBusComp BC = BO.getBusComp("Workflow Action Type");
                        SiebelBusComp BCCHILD = BO.getBusComp("Workflow Action Argument");
                        SiebelBusComp oBCPick = new SiebelBusComp();

                        BC.activateField("Name");
                        BC.clearToQuery();
                        BC.setSearchExpr("[Name] ='" + sAdminC.getNombreaccionpol() + "' ");         // BUSCA PADRE PARA OBTENER ROW_ID DEL AMBIENTE DESTINO
                        BC.executeQuery(true);
                        BC.executeQuery(true);
                        boolean reg = BC.firstRecord();
                        if (reg) {
                            IdPadre = BC.getFieldValue("Id");  // OBTIENE ROW_ID DEL AMBIENTE DESTINO

                        } else {
                            System.out.println("No se encontro registro de Acciones de Poliza con nombre: " + sAdminC.getNombreaccionpol() + " , en el ambiente a insertar, por esta razon no puede validarse si existen ARGUMENTOS a modificar o crear. ");
                            String MsgSalida = "No se encontro registro de Acciones de Poliza con nombre: " + sAdminC.getNombreaccionpol() + " , en el ambiente a insertar, por esta razon no puede validarse si existen ARGUMENTOS a modificar o crear. ";
                            String FlagCarga = "E";
                            String MsgError = "No se encontro registro de Acciones de Poliza con nombre: " + sAdminC.getNombreaccionpol() + " , en el ambiente a insertar, por esta razon no puede validarse si existen ARGUMENTOS a modificar o crear. ";

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgError);
                            this.CargaBitacoraSalidaError(sAdminC.getRowId(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                            BCCHILD.release();
                            BC.release();
                            BO.release();
                        }
                        // SI SE ENCONTRO ROW_ID DE PADRE, COMIENZA BUSQUEDA DE HIJOS 1 - CONDICIONES
                        if (IdPadre != null) {
                            BCCHILD.activateField("Name");
                            BCCHILD.activateField("Required");
                            BCCHILD.activateField("Default Value");
                            BCCHILD.activateField("Action Id"); // Id de Padre
                            BCCHILD.clearToQuery();
                            BCCHILD.setSearchExpr("[Name] ='" + sAdminC.getArgumento() + "' AND [Action Id] ='" + IdPadre + "'");
                            BCCHILD.executeQuery(true);
                            boolean regchild = BCCHILD.firstRecord();
                            if (regchild) {
                                if (sAdminC.getRequerido() != null) {
                                    if (!BCCHILD.getFieldValue("Required").equals(sAdminC.getRequerido())) {
                                        BCCHILD.setFieldValue("Required", sAdminC.getRequerido());
                                    }
                                }

                                if (sAdminC.getValor() != null) {
                                    if (!BCCHILD.getFieldValue("Default Value").equals(sAdminC.getValor())) {
                                        BCCHILD.setFieldValue("Default Value", sAdminC.getValor());
                                    }
                                }

                                BCCHILD.writeRecord();

                                System.out.println("Se valida existencia y/o actualizacion de Hijos Argumento de Accion de Polizas :  " + sAdminC.getArgumento());
                                String MsgSalida = "Se valida existencia y/o actualizacion de Hijos Argumento de Accion de Polizas";
                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(MsgSalida);

                                this.CargaBitacoraSalidaValidado(sAdminC.getRowId(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                BCCHILD.release();
                                BC.release();
                                BO.release();

                            } else {
                                BCCHILD.newRecord(0);

                                if (sAdminC.getArgumento() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("Name");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Name] ='" + sAdminC.getArgumento() + "'");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getRequerido() != null) {
                                    BCCHILD.setFieldValue("Required", sAdminC.getRequerido());
                                }

                                if (sAdminC.getValor() != null) {
                                    BCCHILD.setFieldValue("Default Value", sAdminC.getValor());
                                }

                                BCCHILD.setFieldValue("Action Id", IdPadre);
                                BCCHILD.writeRecord();

                                System.out.println("Se creo Hijo Argumento de Accion de Polizas:  " + sAdminC.getArgumento());
                                String mensaje = ("Se creo Hijo Argumento de Accion de Polizas:  " + sAdminC.getArgumento());
                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(mensaje);

                                String MsgSalida = "Creado Hijo Argumento de Accion de Polizas";
                                this.CargaBitacoraSalidaCreado(sAdminC.getRowId(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                BCCHILD.release();
                                BC.release();
                                BO.release();

                            }
                        }
                        IdPadre = null;

                    } catch (SiebelException e) {
                        String error = e.getErrorMessage();
                        System.out.println("Error en creacion de Hijo Argumento de Accion de Polizas:  " + sAdminC.getArgumento() + "     , con el mensaje:   " + error.replace("'", " "));
                        String MsgSalida = "Error al Crear Hijo Argumento de Accion de Polizas";
                        String FlagCarga = "E";
                        String MsgError = "Error en creacion de Hijo Argumento de Accion de Polizas:  " + sAdminC.getArgumento() + "     , con el mensaje:   " + error.replace("'", " ");

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgError);

                        this.CargaBitacoraSalidaError(sAdminC.getRowId(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

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
        } catch (SiebelException e) {
            String error = e.getErrorMessage();
            System.out.println("Error en  validacion Hijo Argumento de Accion de Polizas:  " + error.replace("'", " "));
            String MsgError = "Error en  validacion Hijo Argumento de Accion de Polizas:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);

        }
    }

    @Override
    public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick, String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws SiebelException {
        try {
            List<AccionesPolizasHijo> hijo = new LinkedList();
            hijo = this.consultaHijo(IdPadre,usuario,version,ambienteExtra,ambienteInser);
            for (AccionesPolizasHijo sAdminC : hijo) {
                try {
                    BC.activateField("Name");
                    BC.activateField("Required");
                    BC.activateField("Default Value");
                    BC.activateField("Action Id"); // Id de Padre
                    BC.clearToQuery();
                    BC.setSearchExpr("[Name] ='" + sAdminC.getArgumento() + "'");
                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {
                        if (sAdminC.getRequerido() != null) {
                            if (!BC.getFieldValue("Required").equals(sAdminC.getRequerido())) {
                                BC.setFieldValue("Required", sAdminC.getRequerido());
                            }
                        }

                        if (sAdminC.getValor() != null) {
                            if (!BC.getFieldValue("Default Value").equals(sAdminC.getValor())) {
                                BC.setFieldValue("Default Value", sAdminC.getValor());
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Argumento de Accion de Polizas :  " + sAdminC.getArgumento());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Argumento de Accion de Polizas";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowId(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getArgumento() != null) {
                            oBCPick = BC.getPicklistBusComp("Name");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name] ='" + sAdminC.getArgumento() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getRequerido() != null) {
                            BC.setFieldValue("Required", sAdminC.getRequerido());
                        }

                        if (sAdminC.getValor() != null) {
                            BC.setFieldValue("Default Value", sAdminC.getValor());
                        }

                        BC.setFieldValue("Action Id", RowID);
                        BC.writeRecord();

                        System.out.println("Se creo Argumento:  " + sAdminC.getArgumento());
                        String mensaje = ("Se creo Argumento:  " + sAdminC.getArgumento());
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

                        String MsgSalida = "Creado Argumento de Accion de Polizas";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowId(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en Argumento de Accion de Polizas:" + sAdminC.getArgumento() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Argumento de Accion de Polizas";
                    String FlagCarga = "E";
                    String MsgError = "Error en Argumento de Accion de Polizas:" + sAdminC.getArgumento() + "     , con el mensaje:   " + error.replace("'", " ");

                    this.CargaBitacoraSalidaError(sAdminC.getRowId(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgError);

                }
            }
        } catch (SiebelException e) {
            String error = e.getErrorMessage();
            System.out.println("Error en cargaBC, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en cargaBC, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);
        } catch (Exception ex) {
            Logger.getLogger(DAOAccionesPolizasImpl.class.getName()).log(Level.SEVERE, null, ex);
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
    public List<AccionesPolizasPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String readRecordSQL = "SELECT A.ROW_ID, A.NAME \"Nombre\", A.PROGRAM_NAME \"Programa\", A.OBJECT_NAME \"Objeto de flujo de trabajo\"\n"
                + "FROM SIEBEL.S_ACTION_DEFN A, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<AccionesPolizasPadre> accionesPolizas = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Padre a procesar.");

            while (rs.next()) {

                AccionesPolizasPadre lista = new AccionesPolizasPadre();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setPrograma(rs.getString("Programa"));
                lista.setObjetoFlujoTrabajo(rs.getString("Objeto de flujo de trabajo"));

                accionesPolizas.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre"), rs.getString("Programa"), rs.getString("Objeto de flujo de trabajo"), "SE OBTIENEN DATOS", "ACCIONES DE POLIZAS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = accionesPolizas.size();
            Boolean conteo = accionesPolizas.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Acciones Polizas Padres o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Acciones Polizas Padres o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Acciones Polizas Padres, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Acciones Polizas Padres, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return accionesPolizas;
    }

    @Override
    public List<AccionesPolizasHijo> consultaHijo(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID,  B.ACTION_ID \"Id Padre\", B.NAME \"Argumento\",  B.REQUIRED \"Requerido\", B.DEFAULT_VALUE \"Valor\"\n"
                + "FROM SIEBEL.S_ACTION_ARG B, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = B.LAST_UPD_BY\n"
                + "AND B.ACTION_ID = '" + IdPadre + "'\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')";
        List<AccionesPolizasHijo> accionesPolizas = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar.");

            while (rs.next()) {
                AccionesPolizasHijo lista = new AccionesPolizasHijo();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setArgumento(rs.getString("Argumento"));
                lista.setRequerido(rs.getString("Requerido"));
                lista.setValor(rs.getString("Valor"));

                accionesPolizas.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Argumento"), rs.getString("Requerido"), rs.getString("Valor"), "SE OBTIENEN DATOS HIJOS", "ACCIONES DE POLIZAS - ARGUMENTO",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = accionesPolizas.size();
            Boolean conteo = accionesPolizas.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Acciones Polizas o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Acciones Polizas o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Acciones Polizas, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Acciones Polizas, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo, con el error:  " + ex);
            throw ex;
        }
        return accionesPolizas;

    }

    @Override
    public List<AccionesPolizasSoloHijos> consultasoloHijos(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID,  B.ACTION_ID \"Id Padre\", B.NAME \"Argumento\",  B.REQUIRED \"Requerido\", B.DEFAULT_VALUE \"Valor\", A.NAME \"Nombre AccionPol\"\n"
                + "FROM SIEBEL.S_ACTION_ARG B, SIEBEL.S_ACTION_DEFN A,SIEBEL.S_USER H\n"
                + "WHERE B.ACTION_ID = A.ROW_ID(+) AND H.ROW_ID = B.LAST_UPD_BY\n"
                + "AND B.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND  H.LOGIN = '" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY B.LAST_UPD ASC";
        List<AccionesPolizasSoloHijos> accionesPolizasSoloHijos = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de Hijos a procesar.");

            while (rs.next()) {
                AccionesPolizasSoloHijos lista = new AccionesPolizasSoloHijos();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setArgumento(rs.getString("Argumento"));
                lista.setRequerido(rs.getString("Requerido"));
                lista.setValor(rs.getString("Valor"));
                lista.setIdpadre(rs.getString("Id Padre"));
                lista.setNombreaccionpol(rs.getString("Nombre AccionPol"));

                accionesPolizasSoloHijos.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Argumento"), rs.getString("Requerido"), rs.getString("Valor"), "SE OBTIENEN DATOS HIJOS", "ACCIONES DE POLIZAS - ARGUMENTO",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = accionesPolizasSoloHijos.size();
            Boolean conteo = accionesPolizasSoloHijos.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Solo Hijos de Acciones Polizas o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Solo Hijos de Acciones Polizas o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Solo Hijos de Acciones Polizas, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Solo Hijos de Acciones Polizas, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en consultasoloHijos, con el error:  " + ex);
            throw ex;
        }
        return accionesPolizasSoloHijos;

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
