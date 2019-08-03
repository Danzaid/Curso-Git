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
import interfaces.DAOListaPolizas;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import objetos.ListaPolizasHijo1;
import objetos.ListaPolizasHijo2;
import objetos.ListaPolizasHijo3;
import objetos.ListaPolizasPadre;
import objetos.ListaPolizasSoloHijo1;
import objetos.ListaPolizasSoloHijo2;

/**
 *
 * @author hector.pineda
 */
public class DAOListaPolizasImpl extends ConexionDB implements DAOListaPolizas {

    @Override
    public void inserta(List<ListaPolizasPadre> listaPolizas, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);

        Boolean conteopadre = listaPolizas.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

            for (ListaPolizasPadre sAdminC : listaPolizas) {  // CREA O ACTUALIZA REGISTROS BUSCANDO DESDE EL PADRE
                if ("SI".equals(Conectar)) {
                    conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                    Conectar = "NO";
                }

                try {

                    String MsgError = "";

                    SiebelBusObject BO = m_dataBean.getBusObject("Workflow Policy");
                    SiebelBusComp BC = BO.getBusComp("Workflow Policy");
                    SiebelBusComp BCCHILD = BO.getBusComp("Workflow Condition");
                    SiebelBusComp BCCHILD2 = BO.getBusComp("Workflow Action");
                    SiebelBusComp BCCHILD3 = BO.getBusComp("Workflow Action Argument");
                    SiebelBusComp oBCPick = new SiebelBusComp();

                    BC.activateField("Name");
                    BC.activateField("Object");
                    BC.activateField("Group");
                    BC.activateField("Time");
                    BC.clearToQuery();
                    BC.setSearchExpr("[Name] ='" + sAdminC.getNombre() + "'");
                    BC.executeQuery(true);
                    boolean reg = BC.firstRecord();
                    if (reg) {

                        if (sAdminC.getObjetoFlujoTrabajo() != null) {
                            if (!BC.getFieldValue("Object").equals(sAdminC.getObjetoFlujoTrabajo())) {
                                oBCPick = BC.getPicklistBusComp("Object");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Name]='" + sAdminC.getObjetoFlujoTrabajo() + "' ");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getGrupoPolizas() != null) {
                            if (!BC.getFieldValue("Group").equals(sAdminC.getGrupoPolizas())) {
                                oBCPick = BC.getPicklistBusComp("Group");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Name]='" + sAdminC.getGrupoPolizas() + "' ");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getDuracion() != null) {
                            if (!BC.getFieldValue("Time").equals(sAdminC.getDuracion())) {
                                BC.setFieldValue("Time", sAdminC.getDuracion());
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion Listas Polizas:  " + sAdminC.getNombre());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Listas Polizas";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

//                        String RowId1 = BC.getFieldValue("Id");
//                        this.cargaBC(BCCHILD, oBCPick, RowId1, sAdminC.getRowId());  //INSERTA CONDICIONES
//                        this.cargaBC2(BCCHILD2, BCCHILD3, oBCPick, RowId1, sAdminC.getRowId());  // INSERTA ACCIONES

                        BCCHILD3.release();
                        BCCHILD2.release();
                        BCCHILD.release();
                        BC.release();
                        BO.release();
                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getNombre() != null) {
                            BC.setFieldValue("Name", sAdminC.getNombre());
                        }

                        if (sAdminC.getObjetoFlujoTrabajo() != null) {
                            oBCPick = BC.getPicklistBusComp("Object");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name]='" + sAdminC.getObjetoFlujoTrabajo() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getGrupoPolizas() != null) {
                            oBCPick = BC.getPicklistBusComp("Group");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name]='" + sAdminC.getGrupoPolizas() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getDuracion() != null) {
                            BC.setFieldValue("Time", sAdminC.getDuracion());
                        }

                        BC.writeRecord();

                        System.out.println("Se crea registro de Listas Polizas: " + sAdminC.getNombre());
                        String mensaje = ("Se crea registro de Listas Polizas: " + sAdminC.getNombre());

                        String MsgSalida = "Creado Accion Polizas";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

//                        String RowID = BC.getFieldValue("Id");
//                        this.cargaBC(BCCHILD, oBCPick, RowID, sAdminC.getRowId());  // INSERTA CONDICIONES
//                        this.cargaBC2(BCCHILD2, BCCHILD3, oBCPick, RowID, sAdminC.getRowId());  // INSERTA ACCIONES

                        BCCHILD3.release();
                        BCCHILD2.release();
                        BCCHILD.release();
                        BC.release();
                        BO.release();

                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en creacion Listas Polizas:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Listas Polizas";
                    String FlagCarga = "E";
                    String MsgError = "Error en creacion Listas Polizas:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " ");

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

        try {                                                                       // BUSCA REGISTROS NUEVOS O ACTUALIZADOS, DESDE EL HIJO 1 - CONDICIONES
            List<ListaPolizasSoloHijo1> hijo = new LinkedList();
            hijo = this.consultaSoloHijo1(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra);

            Boolean conteosolohijo = hijo.isEmpty(); // VALIDA SI LA LISTA ESTA VACIA
            if (!conteosolohijo) {

                ConexionSiebel conSiebel = new ConexionSiebel();
                SiebelDataBean m_dataBean = new SiebelDataBean();
                conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

                String IdPadre = null;

                for (ListaPolizasSoloHijo1 sAdminC : hijo) {  // SI LA LISTA NO ESTA VACIA, COMIENZA EJECUCION
                    if ("SI".equals(Conectar)) {
                        conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                        Conectar = "NO";
                    }
                    try {
                        SiebelBusObject BO = m_dataBean.getBusObject("Workflow Policy");
                        SiebelBusComp BC = BO.getBusComp("Workflow Policy");
                        SiebelBusComp BCCHILD = BO.getBusComp("Workflow Condition");
                        SiebelBusComp oBCPick = new SiebelBusComp();

                        BC.activateField("Name");
                        BC.clearToQuery();
                        BC.setSearchExpr("[Name] ='" + sAdminC.getNombrelistapadre() + "'");  // BUSCA PADRE PARA OBTENER ROW_ID DEL AMBIENTE DESTINO
                        BC.executeQuery(true);
                        boolean reg = BC.firstRecord();
                        if (reg) {
                            IdPadre = BC.getFieldValue("Id");  // OBTIENE ROW_ID DEL AMBIENTE DESTINO
                            
                             // SI SE ENCONTRO ROW_ID DE PADRE, COMIENZA BUSQUEDA DE HIJOS 1 - CONDICIONES
                            BCCHILD.activateField("Condition Column Name");
                            BCCHILD.activateField("Comparison");
                            BCCHILD.activateField("Real Value");
                            BCCHILD.activateField("WF Link Name");
                            BCCHILD.activateField("Rule Id"); // Id de Padre
                            BCCHILD.clearToQuery();
                            BCCHILD.setSearchExpr("[Condition Column Name] ='" + sAdminC.getCampoCondicion() + "' AND [Comparison] = '" + sAdminC.getComparacion() + "' AND [Rule Id] = '" + IdPadre + "'");
                            BCCHILD.executeQuery(true);
                            boolean regchild = BCCHILD.firstRecord();
                            if (regchild) {

                                if (sAdminC.getValor() != null) {
                                    if (!BCCHILD.getFieldValue("Real Value").equals(sAdminC.getValor())) {
                                        BCCHILD.setFieldValue("Real Value", sAdminC.getValor());
                                    }
                                }

                                if (sAdminC.getLinkname() != null) {
                                    if (!BCCHILD.getFieldValue("WF Link Name").equals(sAdminC.getLinkname())) {
                                        BCCHILD.setFieldValue("WF Link Name", sAdminC.getLinkname());
                                    }
                                }

                                BCCHILD.writeRecord();

                                System.out.println("Se valida existencia y/o actualizacion de Hijo - Condiciones:  " + sAdminC.getCampoCondicion() + ":" + sAdminC.getComparacion());
                                String MsgSalida = "Se valida existencia y/o actualizacion de Hijo - Condiciones";
                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(MsgSalida);

                                this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                BCCHILD.release();
                                BC.release();
                                BO.release();

                            } else {
                                BCCHILD.newRecord(0);

                                if (sAdminC.getCampoCondicion() != null) {
                                    BCCHILD.setFieldValue("Condition Column Name", sAdminC.getCampoCondicion());
                                }

                                if (sAdminC.getComparacion() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("Comparison");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Value]='" + sAdminC.getComparacion() + "' ");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getValor() != null) {
                                    BCCHILD.setFieldValue("Real Value", sAdminC.getValor());
                                }

                                if (sAdminC.getLinkname() != null) {
                                    BCCHILD.setFieldValue("WF Link Name", sAdminC.getLinkname());
                                }

                                BCCHILD.setFieldValue("Rule Id", IdPadre);

                                BCCHILD.writeRecord();

                                System.out.println("Se creo Hijo - Condiciones:  " + sAdminC.getCampoCondicion());
                                String mensaje = ("Se creo Hijo - Condiciones:  " + sAdminC.getCampoCondicion());

                                String MsgSalida = "Creado Hijo - Condiciones";
                                this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(mensaje);

                                BCCHILD.release();
                                BC.release();
                                BO.release();
                            }

                        } else {
                            System.out.println("No se encontro registro de Grupo de Poliza con nombre: " + sAdminC.getNombrelistapadre() + " , en el ambiente a insertar, por esta razon no puede validarse si existen CONDICIONES a modificar o crear. ");
                            String MsgSalida = "No se encontro registro de Grupo de Poliza con nombre: " + sAdminC.getNombrelistapadre() + " , en el ambiente a insertar, por esta razon no puede validarse si existen CONDICIONES a modificar o crear. ";
                            String FlagCarga = "E";
                            String MsgError = "No se encontro registro de Grupo de Poliza con nombre: " + sAdminC.getNombrelistapadre() + " , en el ambiente a insertar, por esta razon no puede validarse si existen CONDICIONES a modificar o crear. ";

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgError);
                            this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                            BCCHILD.release();
                            BC.release();
                            BO.release();

                        }


                    } catch (SiebelException e) {
                        String error = e.getErrorMessage();
                        System.out.println("Error en creacion de Hijos - Condiciones:  " + sAdminC.getCampoCondicion() + "     , con el mensaje:   " + error.replace("'", " "));
                        String MsgSalida = "Error al Crear Hijos - Condiciones";
                        String FlagCarga = "E";
                        String MsgError = "Error en creacion de Hijos - Condiciones:  " + sAdminC.getCampoCondicion() + "     , con el mensaje:   " + error.replace("'", " ");

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
            System.out.println("Error en  validacion Solo Hijo - Condiciones de Lista de Polizas, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en  validacion Solo Hijo - Condiciones de Lista de Polizas, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);

        }

        try {                                                                       // BUSCA REGISTROS NUEVOS O ACTUALIZADOS, DESDE EL HIJO 2 - ACCIONES
            List<ListaPolizasSoloHijo2> hijo = new LinkedList();
            hijo = this.consultaSoloHijo2(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra);

            Boolean conteosolohijo2 = hijo.isEmpty(); // VALIDA SI LA LISTA ESTA VACIA
            if (!conteosolohijo2) {

                ConexionSiebel conSiebel = new ConexionSiebel();
                SiebelDataBean m_dataBean = new SiebelDataBean();
                conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

                String IdPadre = null;

                for (ListaPolizasSoloHijo2 sAdminC : hijo) {  // SI LA LISTA NO ESTA VACIA, COMIENZA EJECUCION
                    if ("SI".equals(Conectar)) {
                        conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                        Conectar = "NO";
                    }

                    try {
                        SiebelBusObject BO = m_dataBean.getBusObject("Workflow Policy");
                        SiebelBusComp BC = BO.getBusComp("Workflow Policy");
                        SiebelBusComp BCCHILD = BO.getBusComp("Workflow Action");
                        SiebelBusComp oBCPick = new SiebelBusComp();

                        BC.activateField("Name");
                        BC.clearToQuery();
                        BC.setSearchExpr("[Name] ='" + sAdminC.getNombrepoliza() + "'");  // BUSCA PADRE PARA OBTENER ROW_ID DEL AMBIENTE DESTINO
                        BC.executeQuery(true);
                        boolean reg = BC.firstRecord();
                        if (reg) {
                            IdPadre = BC.getFieldValue("Id");  // OBTIENE ROW_ID DEL AMBIENTE DESTINO
                            
                                // SI SE ENCONTRO ROW_ID DE PADRE, COMIENZA BUSQUEDA DE HIJOS 1 - CONDICIONES
                            BCCHILD.activateField("Action");
                            BCCHILD.activateField("Sequence");
                            BCCHILD.activateField("Rule Id"); // Id de Padre
                            BCCHILD.activateField("Action Id"); // Id para asociar argumentos
                            BCCHILD.clearToQuery();
                            BCCHILD.setSearchExpr("[Action] ='" + sAdminC.getAccion() + "' AND [Rule Id] = '" + IdPadre + "'");
                            BCCHILD.executeQuery(true);
                            boolean regchild = BCCHILD.firstRecord();
                            if (regchild) {

                                if (sAdminC.getSecuencia() != null) {
                                    if (!BCCHILD.getFieldValue("Sequence").equals(sAdminC.getSecuencia())) {
                                        BCCHILD.setFieldValue("Sequence", sAdminC.getSecuencia());
                                    }
                                }

                                BCCHILD.writeRecord();

                                System.out.println("Se valida existencia y/o actualizacion de Hijos - Acciones:  " + sAdminC.getAccion());
                                String MsgSalida = "Se valida existencia y/o actualizacion de Hijos - Acciones";
                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(MsgSalida);

                                this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                BCCHILD.release();
                                BC.release();
                                BO.release();

                            } else {
                                BCCHILD.newRecord(0);

                                if (sAdminC.getAccion() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("Action");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Name]='" + sAdminC.getAccion() + "' ");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getSecuencia() != null) {
                                    BCCHILD.setFieldValue("Sequence", sAdminC.getSecuencia());
                                }

                                BCCHILD.setFieldValue("Rule Id", IdPadre);
                                BCCHILD.writeRecord();

                                System.out.println("Se creo registro de Hijos - Acciones:  " + sAdminC.getAccion());
                                String mensaje = ("Se creo registro de Hijos - Acciones:  " + sAdminC.getAccion());

                                String MsgSalida = "Creado Hijos - Acciones";
                                this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(mensaje);

                                BCCHILD.release();
                                BC.release();
                                BO.release();
                            }


                        } else {
                            System.out.println("No se encontro registro de Lista de Poliza con nombre: " + sAdminC.getNombrepoliza() + " , por esta razon no puede validarse si existen ACCIONES a modificar o crear. ");
                            String MsgSalida = "No se encontro registro de Lista de Poliza con nombre: " + sAdminC.getNombrepoliza() + " , por esta razon no puede validarse si existen ACCIONES a modificar o crear. ";
                            String FlagCarga = "E";
                            String MsgError = "No se encontro registro de Lista de Poliza con nombre: " + sAdminC.getNombrepoliza() + " , por esta razon no puede validarse si existen ACCIONES a modificar o crear. ";

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgError);
                            this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                            BCCHILD.release();
                            BC.release();
                            BO.release();
                        }

                            
                    } catch (SiebelException e) {
                        String error = e.getErrorMessage();
                        System.out.println("Error en creacion de Hijos - Acciones de Grupo de Polizas:  " + sAdminC.getAccion() + "     , con el mensaje:   " + error.replace("'", " "));
                        String MsgSalida = "Error al Crear Hijos - Acciones de Grupo de Polizas";
                        String FlagCarga = "E";
                        String MsgError = "Error en creacion de Hijos - Acciones de Grupo de Polizas:  " + sAdminC.getAccion() + "     , con el mensaje:   " + error.replace("'", " ");

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
            System.out.println("Error en  validacion Hijo - Acciones, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en  validacion Hijo - Acciones, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);

        }
    }

    @Override
    public List<ListaPolizasPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.NAME \"Nombre\", A.OBJECT_NAME \"Objeto de flujo de trabajo\", B.NAME \"Grupo de Pólizas\", A.RULE_DURATION \"Duración\"\n"
                + "FROM SIEBEL.S_ESCL_RULE A, SIEBEL.S_ESCL_GROUP B, SIEBEL.S_USER H\n"
                + "WHERE A.GROUP_ID = B.ROW_ID AND H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<ListaPolizasPadre> listaPolizas = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Padre a procesar.");

            while (rs.next()) {
                ListaPolizasPadre lista = new ListaPolizasPadre();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setObjetoFlujoTrabajo(rs.getString("Objeto de flujo de trabajo"));
                lista.setGrupoPolizas(rs.getString("Grupo de Pólizas"));
                lista.setDuracion(rs.getString("Duración"));
                listaPolizas.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre"), rs.getString("Objeto de flujo de trabajo"), rs.getString("Grupo de Pólizas"), "SE OBTIENEN DATOS", "LISTAS DE POLIZAS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = listaPolizas.size();
            Boolean conteo = listaPolizas.isEmpty(); // Valida si la lista esta vacia
            String mensaje;
            if (conteo) {
                System.out.println("No se obtuvieron listas de Padres Listas Polizas o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Padres Listas Polizaso estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listado de Listas Polizas Padres, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listado de Listas Polizas Padres, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return listaPolizas;
    }

    @Override
    public List<ListaPolizasHijo1> consultaHijo1(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT C.ROW_ID, C.RULE_ID \"Id Padre\", C.LINK_COL_NAME \"Campo de condición\", C.COND_OPERAND \"Comparación\",  C.VAL \"Valor\", C.ESCL_LINK_NAME \"Link Name\"\n"
                + "FROM SIEBEL.S_ESCL_COND C, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = C.LAST_UPD_BY\n"
                + "AND C.RULE_ID = '" + IdPadre + "'\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = C.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')"
                + "ORDER BY C.CREATED ASC";
        List<ListaPolizasHijo1> listaPolizas = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar - Condicion");

            while (rs.next()) {
                ListaPolizasHijo1 lista = new ListaPolizasHijo1();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setCampoCondicion(rs.getString("Campo de condición"));
                lista.setComparacion(rs.getString("Comparación"));
                lista.setValor(rs.getString("Valor"));
                lista.setLinkname(rs.getString("Link Name"));
                listaPolizas.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Campo de condición"), rs.getString("Comparación"), rs.getString("Valor"), "SE OBTIENEN DATOS HIJO - CONDICION", "LISTAS DE POLIZAS - CONDICION",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = listaPolizas.size();
            Boolean conteo = listaPolizas.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Condicion o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Condicion o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Condicion, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Condicion, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo1, con el error:  " + ex);
            throw ex;
        }
        return listaPolizas;
    }

    @Override
    public List<ListaPolizasSoloHijo1> consultaSoloHijo1(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT C.ROW_ID, C.RULE_ID \"Id Padre\", C.LINK_COL_NAME \"Campo de condición\", C.COND_OPERAND \"Comparación\",  C.VAL \"Valor\", C.ESCL_LINK_NAME \"Link Name\", D.NAME \"Nombre Poliza\"\n"
                + "FROM SIEBEL.S_ESCL_COND C, SIEBEL.S_ESCL_RULE D, SIEBEL.S_USER H\n"
                + "WHERE C.RULE_ID =  D.ROW_ID AND H.ROW_ID = C.LAST_UPD_BY\n"
                + "AND C.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND  H.LOGIN = '" + usuario + "' AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA E WHERE E.ROW_ID = C.ROW_ID AND E.USUARIO ='" + usuario + "' AND E.VERSION ='" + version + "' AND E.EXTRAER ='" + ambienteExtra + "' AND E.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY C.LAST_UPD ASC";
        List<ListaPolizasSoloHijo1> listaPolizassolohijo1 = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar - Condicion");

            while (rs.next()) {
                ListaPolizasSoloHijo1 lista = new ListaPolizasSoloHijo1();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setCampoCondicion(rs.getString("Campo de condición"));
                lista.setComparacion(rs.getString("Comparación"));
                lista.setValor(rs.getString("Valor"));
                lista.setLinkname(rs.getString("Link Name"));
                lista.setIdpadre(rs.getString("Id Padre"));
                lista.setNombrelistapadre(rs.getString("Nombre Poliza"));
                listaPolizassolohijo1.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Campo de condición"), rs.getString("Comparación"), rs.getString("Valor"), "SE OBTIENEN DATOS HIJO - CONDICION", "LISTAS DE POLIZAS - CONDICION",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = listaPolizassolohijo1.size();
            Boolean conteo = listaPolizassolohijo1.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Solo Hijos - Condicion o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Solo Hijos Condicion o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Solo Hijos - Condicion, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Solo Hijos Condicion, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaSoloHijo1, con el error:  " + ex);
            throw ex;
        }
        return listaPolizassolohijo1;
    }

    @Override
    public List<ListaPolizasHijo2> consultaHijo2(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT D.ROW_ID,D.RULE_ID \"Id Padre\",F.NAME \"Acción\", D.SEQUENCE \"Secuencia\", D.ACTION_ID \"Id Accion\"\n"
                + "FROM SIEBEL.S_ESCL_ACTION D, SIEBEL.S_ACTION_DEFN F, SIEBEL.S_USER H\n"
                + "WHERE  D.ACTION_ID  = F.ROW_ID AND H.ROW_ID = D.LAST_UPD_BY\n"
                + "AND RULE_ID = '" + IdPadre + "'\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA E WHERE E.ROW_ID = D.ROW_ID AND E.USUARIO ='" + usuario + "' AND E.VERSION ='" + version + "' AND E.EXTRAER ='" + ambienteExtra + "' AND E.INSERTAR ='" + ambienteInser + "')"
                + "ORDER BY D.CREATED ASC";
        List<ListaPolizasHijo2> listaPolizas = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar - Accion");

            while (rs.next()) {
                ListaPolizasHijo2 lista = new ListaPolizasHijo2();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setAccion(rs.getString("Acción"));
                lista.setSecuencia(rs.getString("Secuencia"));
                lista.setIdaccion(rs.getString("Id Accion"));
                listaPolizas.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Acción"), rs.getString("Secuencia"), rs.getString("Id Accion"), "SE OBTIENEN DATOS HIJO - ACCION", "LISTAS DE POLIZAS HIJOS - ACCION",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = listaPolizas.size();
            Boolean conteo = listaPolizas.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Accion o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Accion o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Accion, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Accion, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo2, con el error:  " + ex);
            throw ex;
        }
        return listaPolizas;
    }

    @Override
    public List<ListaPolizasSoloHijo2> consultaSoloHijo2(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT D.ROW_ID,D.RULE_ID \"Id Padre\",F.NAME \"Acción\", D.SEQUENCE \"Secuencia\", D.ACTION_ID \"Id Accion\", G.NAME \"Nombre Poliza\"\n"
                + "FROM SIEBEL.S_ESCL_ACTION D, SIEBEL.S_ACTION_DEFN F, SIEBEL.S_ESCL_RULE G, SIEBEL.S_USER H\n"
                + "WHERE  D.ACTION_ID  = F.ROW_ID (+) AND D.RULE_ID = G.ROW_ID AND H.ROW_ID = D.LAST_UPD_BY\n"
                + "AND D.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA C WHERE C.ROW_ID = D.ROW_ID AND C.USUARIO ='" + usuario + "' AND C.VERSION ='" + version + "' AND C.EXTRAER ='" + ambienteExtra + "' AND C.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY D.LAST_UPD ASC";
        List<ListaPolizasSoloHijo2> listaPolizassolohijo2 = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros de Hijos a procesar - Accion");

            while (rs.next()) {
                ListaPolizasSoloHijo2 lista = new ListaPolizasSoloHijo2();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setAccion(rs.getString("Acción"));
                lista.setSecuencia(rs.getString("Secuencia"));
                lista.setIdaccion(rs.getString("Id Accion"));
                lista.setIdpadre(rs.getString("Id Padre"));
                lista.setNombrepoliza(rs.getString("Nombre Poliza"));
                listaPolizassolohijo2.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Acción"), rs.getString("Secuencia"), rs.getString("Id Accion"), "SE OBTIENEN DATOS HIJO - ACCION", "LISTAS DE POLIZAS HIJOS - ACCION",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = listaPolizassolohijo2.size();
            Boolean conteo = listaPolizassolohijo2.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Solo Hijos - Accion o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Solo Hijos Accion o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Solo Hijos - Accion, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Solo Hijos Accion, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaSoloHijo2, con el error:  " + ex);
            throw ex;
        }
        return listaPolizassolohijo2;
    }

//    @Override
//    public List<ListaPolizasHijo3> consultaHijo3(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
//        String readRecordSQL = "SELECT G.ROW_ID, G.ACTION_ID \"Id Padre\",G.NAME \"Argumento\", G.REQUIRED \"Requerido\", G.DEFAULT_VALUE \"Valor\"\n"
//                + "FROM SIEBEL.S_ACTION_ARG G\n"
//                + "WHERE G.ACTION_ID = '" + IdPadre + "'\n"
//                + "ORDER BY G.CREATED ASC";
//        List<ListaPolizasHijo3> listaPolizas = new LinkedList();
//        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {
//            while (rs.next()) {
//                ListaPolizasHijo3 lista = new ListaPolizasHijo3();
//                lista.setRowid(rs.getString("ROW_ID"));
//                lista.setArgumento(rs.getString("Argumento"));
//                lista.setRequerido(rs.getString("Requerido"));
//                lista.setValor(rs.getString("Valor"));
//
//                listaPolizas.add(lista);
//            }
//            System.out.println("Se obtiene Lista de Hijos: Argumentos de Acciones de Listas de Polizas. ");
//            String mensaje = "Se obtiene Lista de Hijos: Argumentos de Acciones de Listas de Polizas. ";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(mensaje);
//        } catch (SQLException ex) {
//            //Exception
//            throw ex;
//        }//finally{this.CloseDB();}
//        return listaPolizas;
//    }

    @Override
    public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick, String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws SiebelException {
        try {
            List<ListaPolizasHijo1> hijo = new LinkedList();
            hijo = this.consultaHijo1(IdPadre,usuario,version,ambienteInser,ambienteExtra);
            for (ListaPolizasHijo1 sAdminC : hijo) {
                try {
                    BC.activateField("Condition Column Name");
                    BC.activateField("Comparison");
                    BC.activateField("Real Value");
                    BC.activateField("WF Link Name");
                    BC.activateField("Rule Id"); // Id de Padre
                    BC.clearToQuery();
                    BC.setSearchExpr("[Condition Column Name] ='" + sAdminC.getCampoCondicion() + "' AND [Rule Id] = '" + RowID + "'");
                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {
                        if (sAdminC.getComparacion() != null) {
                            if (!BC.getFieldValue("Comparison").equals(sAdminC.getComparacion())) {
                                oBCPick = BC.getPicklistBusComp("Comparison");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Value]='" + sAdminC.getComparacion() + "' ");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getValor() != null) {
                            if (!BC.getFieldValue("Real Value").equals(sAdminC.getValor())) {
                                BC.setFieldValue("Real Value", sAdminC.getValor());
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Condiciones:  " + sAdminC.getCampoCondicion());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Condiciones";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getCampoCondicion() != null) {
                            BC.setFieldValue("Condition Column Name", sAdminC.getCampoCondicion());
                        }

                        if (sAdminC.getComparacion() != null) {
                            oBCPick = BC.getPicklistBusComp("Comparison");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value]='" + sAdminC.getComparacion() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getValor() != null) {
                            BC.setFieldValue("Real Value", sAdminC.getValor());
                        }

                        if (sAdminC.getLinkname() != null) {
                            BC.setFieldValue("WF Link Name", sAdminC.getLinkname());
                        }

                        BC.setFieldValue("Rule Id", RowID);

                        BC.writeRecord();

                        System.out.println("Se creo Condiciones:  " + sAdminC.getCampoCondicion());
                        String mensaje = ("Se creo Condiciones:  " + sAdminC.getCampoCondicion());

                        String MsgSalida = "Creado Condiciones";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);
                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en creacion de Condiciones:  " + sAdminC.getCampoCondicion() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Hijo-Lista de Polizas";
                    String FlagCarga = "E";
                    String MsgError = "Error en creacion de Condiciones:  " + sAdminC.getCampoCondicion() + "     , con el mensaje:   " + error.replace("'", " ");

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgError);

                    this.CargaBitacoraSalidaError(RowID, MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                }
            }
        } catch (SiebelException e) {
            String error = e.getErrorMessage();
            System.out.println("Error en cargaBC, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en cargaBC, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);

        } catch (Exception ex) {
            Logger.getLogger(DAOListaPolizasImpl.class.getName()).log(Level.SEVERE, null, ex);
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
    public void cargaBC2(SiebelBusComp BC, SiebelBusComp BC2, SiebelBusComp oBCPick, String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws SiebelException {
        try {
            List<ListaPolizasHijo2> hijo = new LinkedList();
            hijo = this.consultaHijo2(IdPadre,usuario,version,ambienteInser,ambienteExtra);
            for (ListaPolizasHijo2 sAdminC : hijo) {
                try {
                    BC.activateField("Action");
                    BC.activateField("Sequence");
                    BC.activateField("Rule Id"); // Id de Padre
                    BC.activateField("Action Id"); // Id para asociar argumentos
                    BC.clearToQuery();
                    BC.setSearchExpr("[Action] ='" + sAdminC.getAccion() + "'");
                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {

                        if (sAdminC.getSecuencia() != null) {
                            if (!BC.getFieldValue("Sequence").equals(sAdminC.getSecuencia())) {
                                BC.setFieldValue("Sequence", sAdminC.getSecuencia());
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Acciones:  " + sAdminC.getAccion());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Acciones";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getAccion() != null) {
                            oBCPick = BC.getPicklistBusComp("Action");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name]='" + sAdminC.getAccion() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getSecuencia() != null) {
                            BC.setFieldValue("Sequence", sAdminC.getSecuencia());
                        }

                        BC.setFieldValue("Rule Id", RowID);
                        BC.writeRecord();

                        System.out.println("Se creo Acciones:  " + sAdminC.getAccion());
                        String mensaje = ("Se creo Acciones:  " + sAdminC.getAccion());

                        String MsgSalida = "Creado Acciones";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en creacion de Accion:  " + sAdminC.getAccion() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Hijo2-Lista de Polizas";
                    String FlagCarga = "E";
                    String MsgError = "Error en creacion de Accion:  " + sAdminC.getAccion() + "     , con el mensaje:   " + error.replace("'", " ");

                    this.CargaBitacoraSalidaError(RowID, MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

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
            Logger.getLogger(DAOListaPolizasImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

//    @Override
//    public void cargaBC3(SiebelBusComp BC, SiebelBusComp oBCPick, String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws SiebelException {
//        try {
//            List<ListaPolizasHijo3> hijo = new LinkedList();
//            hijo = this.consultaHijo3(IdPadre,usuario,version,ambienteInser,ambienteExtra);
//            //this.ConectarDB();
//            for (ListaPolizasHijo3 sAdminC : hijo) {
//                try {
//                    BC.activateField("Name");
//                    BC.activateField("Required");
//                    BC.activateField("Default Value");
//                    BC.activateField("Action Id"); // Id de Padre
//                    BC.clearToQuery();
//                    BC.setSearchExpr("[Name] ='" + sAdminC.getArgumento() + "' AND [Default Value] = '" + sAdminC.getValor()
//                            + "' AND [Action Id] = '" + RowID + "' ");
//                    BC.executeQuery(true);
//                    boolean regchild = BC.firstRecord();
//                    if (regchild) {
//
//                        if (sAdminC.getRequerido() != null) {
//                            if (!BC.getFieldValue("Required").equals(sAdminC.getRequerido())) {
//                                BC.setFieldValue("Required", sAdminC.getRequerido());
//                                BC.writeRecord();
//                            }
//                        }
//                    } else {
//                        BC.newRecord(0);
//
//                        if (sAdminC.getArgumento() != null) {
//                            oBCPick = BC.getPicklistBusComp("Name");
//                            oBCPick.clearToQuery();
//                            oBCPick.setSearchSpec("Name", sAdminC.getArgumento());
//                            oBCPick.executeQuery(true);
//                            if (oBCPick.firstRecord()) {
//                                oBCPick.pick();
//                            }
//                            oBCPick.release();
//                        }
//
//                        if (sAdminC.getRequerido() != null) {
//                            BC.setFieldValue("Required", sAdminC.getRequerido());
//                        }
//
//                        if (sAdminC.getValor() != null) {
//                            BC.setFieldValue("Default Value", sAdminC.getValor());
//                        }
//
//                        BC.setFieldValue("Action Id", RowID);
//                        BC.writeRecord();
//
//                        System.out.println("Se creo Argumento:  " + sAdminC.getArgumento());
//                        String mensaje = ("Se creo Argumento:  " + sAdminC.getArgumento());
//                        Reportes rep = new Reportes();
//                        rep.agregarTextoAlfinal(mensaje);
//                    }
//                    //this.updateAdministracionMatrizDescuentos(sAdminC.getRowId());
//                } catch (SiebelException e) {
//                    String error = e.getErrorMessage();
//                    System.out.println("Error en creacion de Argumento:  " + sAdminC.getArgumento() + "     , con el mensaje:   " + error.replace("'", " "));
//                    String MsgSalida = "Error al Crear Hijo3-Lista de Polizas";
//                    String FlagCarga = "E";
//                    String MsgError = "Error en creacion de Argumento:  " + sAdminC.getArgumento() + "     , con el mensaje:   " + error.replace("'", " ");
//
//                    this.CargaBitacoraSalidaError(RowID, MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);
//
//                    Reportes rep = new Reportes();
//                    rep.agregarTextoAlfinal(MsgError);
//
//                }
//            }
//        } catch (SiebelException e) {
//            String error = e.getErrorMessage();
//            System.out.println("Error en cargaBC3, con el error:  " + error.replace("'", " "));
//            String MsgError = "Error en cargaBC3, con el error:  " + error.replace("'", " ");
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(MsgError);
//        } catch (Exception ex) {
//            Logger.getLogger(DAOListaPolizasImpl.class.getName()).log(Level.SEVERE, null, ex);
//        }
//    }

    private void CargaBitacoraEntrada(String Row_Id, String Val1, String Val2, String Val3, String Seguimiento, String Objeto,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

//        String TipoObjeto = Val1 + " : " + Val2 + " : " + Val3;
        String TipoObjeto = Val1 + " : " + Val2;

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
