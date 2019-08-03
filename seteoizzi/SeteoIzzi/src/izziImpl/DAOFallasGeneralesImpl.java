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
import interfaces.DAOFallasGenerales;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import objetos.FallasGeneralesHijo;
import objetos.FallasGeneralesPadre;
import objetos.FallasGeneralesSoloHijo;

/**
 *
 * @author hector.pineda
 */
public class DAOFallasGeneralesImpl extends ConexionDB implements DAOFallasGenerales {

    @Override
    public void inserta(List<FallasGeneralesPadre> fallasGenerales, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);

        Boolean conteopadre = fallasGenerales.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

            for (FallasGeneralesPadre sAdminC : fallasGenerales) {
                if ("SI".equals(Conectar)) {
                    conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                    Conectar = "NO";
                }

                try {

                    String MsgError = "";

                    SiebelBusObject BO = m_dataBean.getBusObject("TT FG Service Request");
                    SiebelBusComp BC = BO.getBusComp("TT Controlador Ordenes Servicio");
                    SiebelBusComp BCCHILD = BO.getBusComp("TT Controlador Bloqueo TC");
                    SiebelBusComp oBCPick = new SiebelBusComp();

                    BC.activateField("TT Category");
                    BC.activateField("TT Reason");
                    BC.activateField("TT Sub Reason");
                    BC.activateField("TT Solution");
                    BC.activateField("TT Semaforo");
                    BC.activateField("TT Time");
                    BC.clearToQuery();
                    BC.setSearchSpec("TT Category", sAdminC.getCategoria());
                    BC.setSearchSpec("TT Reason", sAdminC.getMotivoFG());
                    BC.setSearchSpec("TT Sub Reason", sAdminC.getSubMotivo());
                    BC.setSearchSpec("TT Solution", sAdminC.getSolucion());
                    BC.executeQuery(true);
                    boolean reg = BC.firstRecord();
                    if (reg) {
                        if (sAdminC.getSemaforo() != null) {
                            if (!BC.getFieldValue("TT Semaforo").equals(sAdminC.getSemaforo())) {
                                oBCPick = BC.getPicklistBusComp("TT Semaforo");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Name]='" + sAdminC.getSemaforo() + "' ");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getTiempoSemaforo() != null) {
                            if (!BC.getFieldValue("TT Time").equals(sAdminC.getTiempoSemaforo())) {
                                BC.setFieldValue("TT Time", sAdminC.getTiempoSemaforo());
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion Fallas Generales:  " + sAdminC.getCategoria() + " : " + sAdminC.getMotivoFG() + " : " + sAdminC.getSubMotivo());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Fallas Generales";
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

                        if (sAdminC.getCategoria() != null) {
                            oBCPick = BC.getPicklistBusComp("TT Category");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value]='" + sAdminC.getCategoria() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getMotivoFG() != null) {
                            oBCPick = BC.getPicklistBusComp("TT Reason");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Parent]='" + sAdminC.getCategoria() + "' AND [Value]='" + sAdminC.getMotivoFG() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getSubMotivo() != null) {
                            oBCPick = BC.getPicklistBusComp("TT Sub Reason");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Parent]='" + sAdminC.getMotivoFG() + "' AND [Value]='" + sAdminC.getSubMotivo() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getSolucion() != null) {
                            oBCPick = BC.getPicklistBusComp("TT Solution");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Parent]='" + sAdminC.getSubMotivo() + "' AND [Value]='" + sAdminC.getSolucion() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getSemaforo() != null) {
                            oBCPick = BC.getPicklistBusComp("TT Semaforo");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name]='" + sAdminC.getSemaforo() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getTiempoSemaforo() != null) {
                            BC.setFieldValue("TT Time", sAdminC.getTiempoSemaforo());
                        }

                        BC.writeRecord();

                        System.out.println("Se crea registro de Fallas Generales: " + sAdminC.getCategoria() + " : " + sAdminC.getMotivoFG() + " : " + sAdminC.getSubMotivo());
                        String mensaje = ("Se crea registro de Fallas Generales: " + sAdminC.getCategoria() + " : " + sAdminC.getMotivoFG() + " : " + sAdminC.getSubMotivo());

                        String MsgSalida = "Creado Fallas Generaless";
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
                    System.out.println("Error en creacion Fallas Generales:  " + sAdminC.getCategoria() + " : " + sAdminC.getMotivoFG() + " : " + sAdminC.getSubMotivo() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Fallas Generales";
                    String FlagCarga = "E";
                    String MsgError = "Error en creacion Fallas Generales:  " + sAdminC.getCategoria() + " : " + sAdminC.getMotivoFG() + " : " + sAdminC.getSubMotivo() + "     , con el mensaje:   " + error.replace("'", " ");

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
            List<FallasGeneralesSoloHijo> hijo = new LinkedList();
            hijo = this.consultaSoloHijo(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra);

            Boolean conteosolohijo = hijo.isEmpty(); // VALIDA SI LA LISTA ESTA VACIA
            if (!conteosolohijo) {

                ConexionSiebel conSiebel = new ConexionSiebel();
                SiebelDataBean m_dataBean = new SiebelDataBean();
                conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

                String NombrePadre = "";
                String IdPadre = null;

                for (FallasGeneralesSoloHijo sAdminC : hijo) {  // SI LA LISTA NO ESTA VACIA, COMIENZA EJECUCION
                    if ("SI".equals(Conectar)) {
                        conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                        Conectar = "NO";
                    }
                    try {
                        SiebelBusObject BO = m_dataBean.getBusObject("TT FG Service Request");
                        SiebelBusComp BC = BO.getBusComp("TT Controlador Ordenes Servicio");
                        SiebelBusComp BCCHILD = BO.getBusComp("TT Controlador Bloqueo TC");
                        SiebelBusComp oBCPick = new SiebelBusComp();

                        BC.activateField("TT Category");
                        BC.activateField("TT Reason");
                        BC.activateField("TT Sub Reason");
                        BC.activateField("TT Solution");
                        BC.clearToQuery();
                        BC.setSearchSpec("TT Category", sAdminC.getCategoria());
                        BC.setSearchSpec("TT Reason", sAdminC.getMotivofg());
                        BC.setSearchSpec("TT Sub Reason", sAdminC.getSubmotivo());
                        BC.setSearchSpec("TT Solution", sAdminC.getSolucion());             // BUSCA PADRE PARA OBTENER ROW_ID DEL AMBIENTE DESTINO  
                        BC.executeQuery(true);
                        boolean reg = BC.firstRecord();
                        if (reg) {
                            IdPadre = BC.getFieldValue("Id");  // OBTIENE ROW_ID DEL AMBIENTE DESTINO

                        } else {
                            System.out.println("No se encontro registro de Fallas Generales Padre en el ambiente a insertar, por esta razon no puede validarse si existen BLOQUEOS a modificar o crear. ");
                            String MsgSalida = "No se encontro registro de Fallas Generales Padre en el ambiente a insertar, por esta razon no puede validarse si existen BLOQUEOS a modificar o crear. ";
                            String FlagCarga = "E";
                            String MsgError = "No se encontro registro de Fallas Generales Padre en el ambiente a insertar, por esta razon no puede validarse si existen BLOQUEOS a modificar o crear. ";

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgError);
                            this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                            BCCHILD.release();
                            BC.release();
                            BO.release();
                        }

                        if (IdPadre != null) {                          // SI SE ENCONTRO ROW_ID DE PADRE, COMIENZA BUSQUEDA DE HIJOS 1 - BLOQUEO DE TROUBLE CALL
                            BC.activateField("TT Tipo Orden");
                            BCCHILD.activateField("TT Motivo Orden");
                            BCCHILD.activateField("TT Parent Id"); // Id de Padre
                            BCCHILD.clearToQuery();
                            BCCHILD.setSearchSpec("TT Tipo Orden", sAdminC.getTipoOrden());
                            BCCHILD.setSearchSpec("TT Motivo Orden", sAdminC.getMotivoOrden());
                            BCCHILD.setSearchSpec("TT Parent Id", IdPadre);
                            BCCHILD.executeQuery(true);
                            boolean regchild = BCCHILD.firstRecord();
                            if (regchild) {

                                System.out.println("Se valida existencia y/o actualizacion de Bloqueo de Fallas Generales:  " + sAdminC.getTipoOrden() + " : " + sAdminC.getMotivoOrden());
                                String MsgSalida = "Se valida existencia y/o actualizacion de Bloqueo de Fallas Generales";
                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(MsgSalida);

                                this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                BCCHILD.release();
                                BC.release();
                                BO.release();

                            } else {
                                BCCHILD.newRecord(0);

                                if (sAdminC.getTipoOrden() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("TT Tipo Orden");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Order Type]='" + sAdminC.getTipoOrden() + "' ");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getMotivoOrden() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("TT Motivo Orden");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Parent]='" + sAdminC.getTipoOrden() + "' AND [Value]='" + sAdminC.getMotivoOrden() + "'");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                BCCHILD.setFieldValue("TT Parent Id", IdPadre);

                                BCCHILD.writeRecord();

                                System.out.println("Se creo Bloqueo de Fallas Generales:  " + sAdminC.getTipoOrden() + " : " + sAdminC.getMotivoOrden());
                                String mensaje = ("Se creo Bloqueo de Fallas Generales:  " + sAdminC.getTipoOrden() + " : " + sAdminC.getMotivoOrden());
                                String MsgSalida = "Creado Bloqueo de Fallas Generales";

                                this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(mensaje);

                                BCCHILD.release();
                                BC.release();
                                BO.release();

                            }
                        }
                        IdPadre = null;

                    } catch (SiebelException e) {
                        String error = e.getErrorMessage();
                        System.out.println("Error en creacion de Hijo de Fallas Generales:  " + sAdminC.getTipoOrden() + " : " + sAdminC.getMotivoOrden() + "     , con el mensaje:   " + error.replace("'", " "));
                        String MsgSalida = "Error al Crear Hijo de Fallas Generales";
                        String FlagCarga = "E";
                        String MsgError = "Error en creacion de Hijo de Fallas Generales:  " + sAdminC.getTipoOrden() + " : " + sAdminC.getMotivoOrden() + "     , con el mensaje:   " + error.replace("'", " ");

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgError);

                        this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

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
            System.out.println("Error en  validacion Solo Hijo - Fallas Generales, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en  validacion Solo Hijo - Fallas Generales, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);

        }
    }

    @Override
    public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick, String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws SiebelException {
        try {
            List<FallasGeneralesHijo> hijo = new LinkedList();
            hijo = this.consultaHijo(IdPadre,usuario,version,ambienteInser,ambienteExtra);
            for (FallasGeneralesHijo sAdminC : hijo) {
                try {
                    BC.activateField("TT Tipo Orden");
                    BC.activateField("TT Motivo Orden");
                    BC.activateField("TT Parent Id"); // Id de Padre
                    BC.clearToQuery();
                    BC.setSearchSpec("TT Tipo Orden", sAdminC.getTipoOrden());
                    BC.setSearchSpec("TT Motivo Orden", sAdminC.getMotivoOrden());
                    BC.setSearchSpec("TT Parent Id", RowID);
                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {

                        System.out.println("Se valida existencia y/o actualizacion de Bloqueo de Fallas Generales:  " + sAdminC.getTipoOrden() + " : " + sAdminC.getMotivoOrden());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Bloqueo de Fallas Generales";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getTipoOrden() != null) {
                            oBCPick = BC.getPicklistBusComp("TT Tipo Orden");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Order Type]='" + sAdminC.getTipoOrden() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getMotivoOrden() != null) {
                            oBCPick = BC.getPicklistBusComp("TT Motivo Orden");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Parent]='" + sAdminC.getTipoOrden() + "' AND [Value]='" + sAdminC.getMotivoOrden() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        BC.setFieldValue("TT Parent Id", RowID);

                        BC.writeRecord();

                        System.out.println("Se creo Bloqueo de Fallas Generales:  " + sAdminC.getTipoOrden() + " : " + sAdminC.getMotivoOrden());
                        String mensaje = ("Se creo Bloqueo de Fallas Generales:  " + sAdminC.getTipoOrden() + " : " + sAdminC.getMotivoOrden());
                        String MsgSalida = "Creado Bloqueo de Fallas Generales";

                        this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    String error2 = e.getDetailedMessage();
                    System.out.println("Error en Bloqueo Fallas Generales:" + sAdminC.getTipoOrden() + "     , con el mensaje:   " + error2.replace("'", " "));
                    String MsgSalida = "Error al Crear Bloqueo Fallas Generales";
                    String FlagCarga = "E";
                    String MsgError = "Error en Bloqueo Fallas Generales:" + sAdminC.getTipoOrden() + "     , con el mensaje:   " + error2.replace("'", " ");

                    this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

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
            Logger.getLogger(DAOFallasGeneralesImpl.class.getName()).log(Level.SEVERE, null, ex);
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
    public List<FallasGeneralesPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.TT_CATEGORY \"Categoria\", A.TT_REASON \"Motivo FG\", A.TT_SUB_REASON \"Sub Motivo\", A.TT_SOLUTION \"Solucion\", A.TT_COLOR \"Semaforo\", A.TT_TIME \"Tiempo Semaforo (hrs)\"\n"
                + "FROM SIEBEL.CX_TT_CTRL_OS A, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND TT_TYPE = 'Tipificacion' AND A.CREATED BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.CREATED ASC";
        List<FallasGeneralesPadre> fallasGenerales = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Padre a procesar.");

            while (rs.next()) {
                FallasGeneralesPadre lista = new FallasGeneralesPadre();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setCategoria(rs.getString("Categoria"));
                lista.setMotivoFG(rs.getString("Motivo FG"));
                lista.setSubMotivo(rs.getString("Sub Motivo"));
                lista.setSolucion(rs.getString("Solucion"));
                lista.setSemaforo(rs.getString("Semaforo"));
                lista.setTiempoSemaforo(rs.getString("Tiempo Semaforo (hrs)"));
                fallasGenerales.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Categoria"), rs.getString("Motivo FG"), rs.getString("Sub Motivo"), "SE OBTIENEN DATOS", "FALLAS GENERALES",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = fallasGenerales.size();
            Boolean conteo = fallasGenerales.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Padres de Fallas Generales o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Padres de Fallas Generales o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Padres de Fallas Generales, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Padres de Fallas Generales, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return fallasGenerales;
    }

    @Override
    public List<FallasGeneralesHijo> consultaHijo(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.TT_PAR_ROW_ID \"Id Padre\", B.NAME \"Tipo Orden\", A.TT_MOTIVO \"Motivo Orden\", A.TT_TYPE\n"
                + "FROM SIEBEL.CX_TT_CTRL_OS A, SIEBEL.S_ORDER_TYPE B, SIEBEL.S_USER H\n"
                + "WHERE A.TT_ORDER_TYPE =B. ROW_ID AND H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND TT_TYPE = 'Bloqueo' AND A.TT_PAR_ROW_ID = '" + IdPadre + "'\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')"
                + "ORDER BY A.CREATED ASC";
        List<FallasGeneralesHijo> fallasGenerales = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar.");

            while (rs.next()) {
                FallasGeneralesHijo lista = new FallasGeneralesHijo();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setTipoOrden(rs.getString("Tipo Orden"));
                lista.setMotivoOrden(rs.getString("Motivo Orden"));
                fallasGenerales.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Tipo Orden"), rs.getString("Motivo Orden"), "", "SE OBTIENEN DATOS HIJO", "FALLAS GENERALES - BLOQUEOS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = fallasGenerales.size();
            Boolean conteo = fallasGenerales.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Fallas Generales o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Fallas Generales o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Fallas Generales, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Fallas Generales, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo, con el error:  " + ex);
            throw ex;
        }
        return fallasGenerales;
    }

    @Override
    public List<FallasGeneralesSoloHijo> consultaSoloHijo(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.TT_PAR_ROW_ID \"Id Padre\", B.NAME \"Tipo Orden\", A.TT_MOTIVO \"Motivo Orden\", G.TT_CATEGORY \"Categoria\",G.TT_REASON \"Motivo\",  G.TT_SUB_REASON \"Sub Motivo\", G.TT_SOLUTION \"Solucion\"\n"
                + "FROM SIEBEL.CX_TT_CTRL_OS A, SIEBEL.S_ORDER_TYPE B, SIEBEL.CX_TT_CTRL_OS G, SIEBEL.S_USER H\n"
                + "WHERE A.TT_ORDER_TYPE = B. ROW_ID AND A.TT_PAR_ROW_ID = G.ROW_ID AND H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<FallasGeneralesSoloHijo> fallasGeneralessolohijo = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar.");

            while (rs.next()) {
                FallasGeneralesSoloHijo lista = new FallasGeneralesSoloHijo();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setTipoOrden(rs.getString("Tipo Orden"));
                lista.setMotivoOrden(rs.getString("Motivo Orden"));
                lista.setCategoria(rs.getString("Categoria"));
                lista.setMotivofg(rs.getString("Motivo"));
                lista.setSubmotivo(rs.getString("Sub Motivo"));
                lista.setSolucion(rs.getString("Solucion"));
                fallasGeneralessolohijo.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Tipo Orden"), rs.getString("Motivo Orden"), "", "SE OBTIENEN DATOS HIJO", "FALLAS GENERALES - BLOQUEOS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = fallasGeneralessolohijo.size();
            Boolean conteo = fallasGeneralessolohijo.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Fallas Generales o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Fallas Generales o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Fallas Generales, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Fallas Generales, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaSoloHijo, con el error:  " + ex);
            throw ex;
        }
        return fallasGeneralessolohijo;
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
