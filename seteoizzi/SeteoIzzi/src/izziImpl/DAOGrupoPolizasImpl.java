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
import interfaces.DAOGrupoPolizas;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import objetos.GrupoPolizasHijo;
import objetos.GrupoPolizasPadre;
import objetos.GrupoPolizasSoloHijo;

/**
 *
 * @author hector.pineda
 */
public class DAOGrupoPolizasImpl extends ConexionDB implements DAOGrupoPolizas {

    @Override
    public void inserta(List<GrupoPolizasPadre> grupoPolizas, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);

        Boolean conteopadre = grupoPolizas.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

            for (GrupoPolizasPadre sAdminC : grupoPolizas) {
                if ("SI".equals(Conectar)) {
                    conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                    Conectar = "NO";
                }

                try {

                    String MsgError = "";
                    SiebelBusObject BO = m_dataBean.getBusObject("Workflow Group");
                    SiebelBusComp BC = BO.getBusComp("Workflow Group");
                    SiebelBusComp BCCHILD = BO.getBusComp("Workflow Policy");
                    SiebelBusComp oBCPick = new SiebelBusComp();

                    BC.activateField("Name");
                    BC.activateField("Comments");
                    BC.clearToQuery();
                    BC.setSearchExpr("[Name] ='" + sAdminC.getNombreGrupoPoliza() + "' ");
                    BC.executeQuery(true);
                    boolean reg = BC.firstRecord();
                    if (reg) {

                        if (sAdminC.getDescripcion() != null) {
                            if (!BC.getFieldValue("Comments").equals(sAdminC.getDescripcion())) {
                                BC.setFieldValue("Comments", sAdminC.getDescripcion());
                            }
                        }
                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion Grupo de Polizas:  " + sAdminC.getNombreGrupoPoliza());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Grupo de Polizas";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

//                        String RowId1 = BC.getFieldValue("Id");
//                        this.cargaBC(BCCHILD, oBCPick, RowId1, sAdminC.getRowid(),usuario,version,ambienteInser,ambienteExtra);

                        BCCHILD.release();
                        BC.release();
                        BO.release();

                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getNombreGrupoPoliza() != null) {
                            BC.setFieldValue("Name", sAdminC.getNombreGrupoPoliza());
                        }

                        if (sAdminC.getDescripcion() != null) {
                            BC.setFieldValue("Comments", sAdminC.getDescripcion());
                        }

                        BC.writeRecord();

                        System.out.println("Se crea registro de Grupo de Polizas: " + sAdminC.getNombreGrupoPoliza());
                        String mensaje = ("Se crea registro de Grupo de Polizas: " + sAdminC.getNombreGrupoPoliza());

                        String MsgSalida = "Creado Grupo de Polizas";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

//                        String RowID = BC.getFieldValue("Id"); // Obtiene Id de registro creado                    
//                        this.cargaBC(BCCHILD, oBCPick, RowID, sAdminC.getRowid(),usuario,version,ambienteInser,ambienteExtra);

                        BCCHILD.release();
                        BC.release();
                        BO.release();

                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en creacion Grupo de Polizas:  " + sAdminC.getNombreGrupoPoliza() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Grupo de Polizas";
                    String FlagCarga = "E";
                    String MsgError = "Error en creacion Grupo de Polizas:  " + sAdminC.getNombreGrupoPoliza() + "     , con el mensaje:   " + error.replace("'", " ");

                    this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);
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

        try {                                                                       // BUSCA REGISTROS NUEVOS O ACTUALIZADOS, DESDE EL HIJO - POLIZAS
            List<GrupoPolizasSoloHijo> hijo = new LinkedList();
            hijo = this.consultaSoloHijo(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra);

            Boolean conteosolohijo = hijo.isEmpty(); // VALIDA SI LA LISTA ESTA VACIA
            if (!conteosolohijo) {

                ConexionSiebel conSiebel = new ConexionSiebel();
                SiebelDataBean m_dataBean = new SiebelDataBean();
                conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

                String IdPadre = null;

                for (GrupoPolizasSoloHijo sAdminC : hijo) {  // SI LA LISTA NO ESTA VACIA, COMIENZA EJECUCION
                    if ("SI".equals(Conectar)) {
                        conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                        Conectar = "NO";
                    }
                    try {
                        SiebelBusObject BO = m_dataBean.getBusObject("Workflow Group");
                        SiebelBusComp BC = BO.getBusComp("Workflow Group");
                        SiebelBusComp BCCHILD = BO.getBusComp("Workflow Policy");
                        SiebelBusComp oBCPick = new SiebelBusComp();

                        BC.activateField("Name");
                        BC.activateField("Comments");
                        BC.clearToQuery();
                        BC.setSearchExpr("[Name] ='" + sAdminC.getNombregpopoliza() + "' ");  // BUSCA PADRE PARA OBTENER ROW_ID DEL AMBIENTE DESTINO
                        BC.executeQuery(true);
                        boolean reg = BC.firstRecord();
                        if (reg) {
                            IdPadre = BC.getFieldValue("Id");  // OBTIENE ROW_ID DEL AMBIENTE DESTINO
                            
                               // SI SE ENCONTRO ROW_ID DE PADRE, COMIENZA BUSQUEDA DE HIJOS 1 - POLIZAS

                            BC.activateField("Name");
                            BCCHILD.activateField("Object");
                            BCCHILD.activateField("Activation Date/Time");
                            BCCHILD.activateField("Expiration Date/Time");
                            BCCHILD.activateField("Comments");
                            BCCHILD.activateField("Group Id");
                            BCCHILD.clearToQuery();
                            BCCHILD.setSearchExpr("[Name] ='" + sAdminC.getNombre() + "' AND [Group Id] ='" + IdPadre + "'");
                            BCCHILD.executeQuery(true);
                            boolean regchild = BCCHILD.firstRecord();
                            if (regchild) {

                                if (sAdminC.getObjeto() != null) {
                                    if (!BCCHILD.getFieldValue("Object").equals(sAdminC.getObjeto())) {
                                        oBCPick = BCCHILD.getPicklistBusComp("Object");
                                        oBCPick.clearToQuery();
                                        oBCPick.setSearchSpec("Name", sAdminC.getObjeto());
                                        oBCPick.executeQuery(true);
                                        if (oBCPick.firstRecord()) {
                                            oBCPick.pick();
                                        }
                                        oBCPick.release();
                                    }
                                }

                                if (sAdminC.getActivacion() != null) {
                                    if (!BCCHILD.getFieldValue("Activation Date/Time").equals(sAdminC.getActivacion())) {
                                        SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                                        String dateString = format.format(sAdminC.getActivacion());
                                        BCCHILD.setFieldValue("Activation Date/Time", dateString);
                                    }
                                }

                                if (sAdminC.getVencimiento() != null) {
                                    if (!BCCHILD.getFieldValue("Expiration Date/Time").equals(sAdminC.getVencimiento())) {
                                        SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                                        String dateString = format.format(sAdminC.getVencimiento());
                                        BCCHILD.setFieldValue("Expiration Date/Time", dateString);
                                    }
                                }

                                if (sAdminC.getComentarios() != null) {
                                    if (!BCCHILD.getFieldValue("Comments").equals(sAdminC.getComentarios())) {
                                        BCCHILD.setFieldValue("Comments", sAdminC.getComentarios());
                                    }
                                }
                                BCCHILD.writeRecord();

                                System.out.println("Se valida existencia y/o actualizacion de Hijo - Poliza:  " + sAdminC.getNombre());
                                String MsgSalida = "Se valida existencia y/o actualizacion de Hijo - Poliza:";
                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(MsgSalida);

                                this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);
                                
                                BCCHILD.release();
                                BC.release();
                                BO.release();

                            } else {
                                BCCHILD.newRecord(0);

                                if (sAdminC.getNombre() != null) {
                                    BCCHILD.setFieldValue("Name", sAdminC.getNombre());
                                }

                                if (sAdminC.getObjeto() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("Object");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchSpec("Name", sAdminC.getObjeto());
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getActivacion() != null) {
                                    SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                                    String dateString = format.format(sAdminC.getActivacion());
                                    BCCHILD.setFieldValue("Activation Date/Time", dateString);
                                }

                                if (sAdminC.getVencimiento() != null) {
                                    SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                                    String dateString = format.format(sAdminC.getVencimiento());
                                    BCCHILD.setFieldValue("Expiration Date/Time", dateString);
                                }

                                if (sAdminC.getComentarios() != null) {
                                    BCCHILD.setFieldValue("Comments", sAdminC.getComentarios());
                                }

                                BCCHILD.setFieldValue("Group Id", IdPadre);

                                BCCHILD.writeRecord();

                                System.out.println("Se creo Hijo - Poliza:  " + sAdminC.getNombre());
                                String mensaje = ("Se creo Hijo - Poliza:  " + sAdminC.getNombre());

                                String MsgSalida = "Creado Hijo - Poliza";
                                this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(mensaje);
                                
                                BCCHILD.release();
                                BC.release();
                                BO.release();
                            }

                        } else {
                            System.out.println("No se encontro registro de Grupo de Polizas con nombre: " + sAdminC.getNombregpopoliza() + " , en el ambiente a insertar, por esta razon no puede validarse si existen POLIZAS a modificar o crear. ");
                            String MsgSalida = "No se encontro registro de Grupo de Polizas con nombre: " + sAdminC.getNombregpopoliza() + " , en el ambiente a insertar, por esta razon no puede validarse si existen POLIZAS a modificar o crear. ";
                            String FlagCarga = "E";
                            String MsgError = "No se encontro registro de Grupo de Polizas con nombre: " + sAdminC.getNombregpopoliza() + " , en el ambiente a insertar, por esta razon no puede validarse si existen POLIZAS a modificar o crear. ";

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgError);
                            this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                            BC.release();
                            
                            BCCHILD.release();
                            BC.release();
                            BO.release();

                        }
                             


                    } catch (SiebelException e) {
                        String error = e.getErrorMessage();
                        System.out.println("Error en creacion de Hijo - Poliza:  " + sAdminC.getNombre() + " : " + "     , con el mensaje:   " + error.replace("'", " "));
                        String MsgSalida = "Error al Crear Hijo - Poliza";
                        String FlagCarga = "E";
                        String MsgError = "Error en creacion de Hijo - Poliza:  " + sAdminC.getNombre() + " : "  + "     , con el mensaje:   " + error.replace("'", " ");

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
            System.out.println("Error en  validacion Solo Hijo - Grupo de Polizas, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en  validacion Solo Hijo - Grupo de Polizas, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);

        }
    }

    @Override
    public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick, String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws SiebelException {
        try {
            List<GrupoPolizasHijo> hijo = new LinkedList();
            hijo = this.consultaHijo(IdPadre,usuario,version,ambienteInser,ambienteExtra);
            for (GrupoPolizasHijo sAdminC : hijo) {
                try {
                    BC.activateField("Name");
                    BC.activateField("Object");
                    BC.activateField("Activation Date/Time");
                    BC.activateField("Expiration Date/Time");
                    BC.activateField("Comments");
                    BC.activateField("Group Id");
                    BC.clearToQuery();
                    BC.setSearchExpr("[Name] ='" + sAdminC.getNombre() + "' ");
                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {

                        if (sAdminC.getObjeto() != null) {
                            if (!BC.getFieldValue("Object").equals(sAdminC.getObjeto())) {
                                oBCPick = BC.getPicklistBusComp("Object");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchSpec("Name", sAdminC.getObjeto());
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getActivacion() != null) {
                            if (!BC.getFieldValue("Activation Date/Time").equals(sAdminC.getActivacion())) {
                                SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                                String dateString = format.format(sAdminC.getActivacion());
                                BC.setFieldValue("Activation Date/Time", dateString);
                            }
                        }

                        if (sAdminC.getVencimiento() != null) {
                            if (!BC.getFieldValue("Expiration Date/Time").equals(sAdminC.getVencimiento())) {
                                SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                                String dateString = format.format(sAdminC.getVencimiento());
                                BC.setFieldValue("Expiration Date/Time", dateString);
                            }
                        }

                        if (sAdminC.getComentarios() != null) {
                            if (!BC.getFieldValue("Comments").equals(sAdminC.getComentarios())) {
                                BC.setFieldValue("Comments", sAdminC.getComentarios());
                            }
                        }
                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Hijo - Poliza :  " + sAdminC.getNombre());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Hijo - Poliza";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getNombre() != null) {
                            BC.setFieldValue("Name", sAdminC.getNombre());
                        }

                        if (sAdminC.getObjeto() != null) {
                            oBCPick = BC.getPicklistBusComp("Object");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchSpec("Name", sAdminC.getObjeto());
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getActivacion() != null) {
                            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                            String dateString = format.format(sAdminC.getActivacion());
                            BC.setFieldValue("Activation Date/Time", dateString);
                        }

                        if (sAdminC.getVencimiento() != null) {
                            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                            String dateString = format.format(sAdminC.getVencimiento());
                            BC.setFieldValue("Expiration Date/Time", dateString);
                        }

                        if (sAdminC.getComentarios() != null) {
                            BC.setFieldValue("Comments", sAdminC.getComentarios());
                        }

                        BC.setFieldValue("Group Id", RowID);

                        BC.writeRecord();

                        System.out.println("Se creo Hijo - Poliza:  " + sAdminC.getNombre());
                        String mensaje = ("Se creo Hijo - Poliza:  " + sAdminC.getNombre());

                        String MsgSalida = "Creado WF Policy";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);
                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en Hijo - Poliza:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Hijo - Poliza";
                    String FlagCarga = "E";
                    String MsgError = "Error en Hijo - Poliza:" + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " ");

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
            Logger.getLogger(DAOGrupoPolizasImpl.class.getName()).log(Level.SEVERE, null, ex);
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
    public List<GrupoPolizasPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.NAME \"Nombre del grupo de pólizas\", A.COMMENTS \"Descripción\"\n"
                + "FROM SIEBEL.S_ESCL_GROUP A, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<GrupoPolizasPadre> grupoPolizas = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Padre a procesar.");

            while (rs.next()) {
                GrupoPolizasPadre lista = new GrupoPolizasPadre();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setNombreGrupoPoliza(rs.getString("Nombre del grupo de pólizas"));
                lista.setDescripcion(rs.getString("Descripción"));
                grupoPolizas.add(lista);

                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre del grupo de pólizas"), rs.getString("Descripción"), "", "SE OBTIENEN DATOS", "GRUPO DE POLIZAS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = grupoPolizas.size();
            Boolean conteo = grupoPolizas.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Grupo de Polizas o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Grupo de Polizas o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Grupo de Polizas, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Grupo de Polizas, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return grupoPolizas;
    }

    @Override
    public List<GrupoPolizasHijo> consultaHijo(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.GROUP_ID \"Id Padre\", B.NAME \"Nombre\", B.OBJECT_NAME \"Objeto\", B.ACTIVATE_DT \"Activacion\", B.EXPIRE_DT \"Vencimiento\", B.COMMENTS \"Comentarios\"\n"
                + "FROM SIEBEL.S_ESCL_RULE B, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = B.LAST_UPD_BY"
                + "AND B.GROUP_ID = '" + IdPadre + "'\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')"
                + "ORDER BY B.CREATED ASC";
        List<GrupoPolizasHijo> grupoPolizas = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar.");

            while (rs.next()) {
                GrupoPolizasHijo lista = new GrupoPolizasHijo();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setObjeto(rs.getString("Objeto"));
                lista.setActivacion(rs.getDate("Activacion"));
                lista.setVencimiento(rs.getDate("Vencimiento"));
                lista.setComentarios(rs.getString("Comentarios"));
                grupoPolizas.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre"), rs.getString("Objeto"), "", "SE OBTIENEN DATOS HIJO", "GRUPO DE POLIZAS - POLIZAS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = grupoPolizas.size();
            Boolean conteo = grupoPolizas.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Polizas o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos - Polizas o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Polizas, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos - Polizas, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo, con el error:  " + ex);
            throw ex;
        }
        return grupoPolizas;
    }

    @Override
    public List<GrupoPolizasSoloHijo> consultaSoloHijo(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.GROUP_ID \"Id Padre\", B.NAME \"Nombre\", B.OBJECT_NAME \"Objeto\", B.ACTIVATE_DT \"Activacion\", B.EXPIRE_DT \"Vencimiento\", B.COMMENTS \"Comentarios\", A.NAME \"Nombre Gpo Poliza\"\n"
                + "FROM SIEBEL.S_ESCL_RULE B, SIEBEL.S_ESCL_GROUP A, SIEBEL.S_USER H\n"
                + "WHERE B.GROUP_ID = A.ROW_ID AND H.ROW_ID = B.LAST_UPD_BY\n"
                + "AND B.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY B.LAST_UPD ASC";
        List<GrupoPolizasSoloHijo> grupoPolizassolohijo = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar.");

            while (rs.next()) {
                GrupoPolizasSoloHijo lista = new GrupoPolizasSoloHijo();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setObjeto(rs.getString("Objeto"));
                lista.setActivacion(rs.getDate("Activacion"));
                lista.setVencimiento(rs.getDate("Vencimiento"));
                lista.setComentarios(rs.getString("Comentarios"));
                lista.setIdpadre(rs.getString("Id Padre"));
                lista.setNombregpopoliza(rs.getString("Nombre Gpo Poliza"));
                grupoPolizassolohijo.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre"), rs.getString("Objeto"), "", "SE OBTIENEN DATOS HIJO", "GRUPO DE POLIZAS - POLIZAS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = grupoPolizassolohijo.size();
            Boolean conteo = grupoPolizassolohijo.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Polizaso estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos - Polizas o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Polizas, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos - Polizas, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaSoloHijo, con el error:  " + ex);
            throw ex;
        }
        return grupoPolizassolohijo;
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
