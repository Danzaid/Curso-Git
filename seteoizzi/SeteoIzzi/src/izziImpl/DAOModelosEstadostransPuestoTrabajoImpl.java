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
import interfaces.DAOModelosEstadostransPuestoTrabajo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import objetos.ModelosEstadostransPuestoTrabajoHijo1;
import objetos.ModelosEstadostransPuestoTrabajoHijo2;
//import objetos.ModelosEstadostransPuestoTrabajoHijo3;
import objetos.ModelosEstadostransPuestoTrabajoPadre;
import objetos.ModelosEstadostransPuestoTrabajoSoloHijo1;
import objetos.ModelosEstadostransPuestoTrabajoSoloHijo2;

/**
 *
 * @author hector.pineda
 */
public class DAOModelosEstadostransPuestoTrabajoImpl extends ConexionDB implements DAOModelosEstadostransPuestoTrabajo {

    @Override
    public void inserta(List<ModelosEstadostransPuestoTrabajoPadre> listaModelosEstadostransPuestoTrabajo, String it, String fechaIni, String fechaTer, String url, String usuarioconn, String passw, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);

        Boolean conteopadre = listaModelosEstadostransPuestoTrabajo.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI", url, usuarioconn, passw);

            for (ModelosEstadostransPuestoTrabajoPadre sAdminC : listaModelosEstadostransPuestoTrabajo) {
                if ("SI".equals(Conectar)) {
                    conSiebel.ConexionSiebel(m_dataBean, Conectar, url, usuarioconn, passw);
                    Conectar = "NO";
                }

                try {

                    String MsgError = "";
                    SiebelBusObject BO = m_dataBean.getBusObject("State Model");
                    SiebelBusObject BOEng = m_dataBean.getBusObject("State Model - Engine");
                    SiebelBusComp BC = BO.getBusComp("State Model");
                    SiebelBusComp BCCHILD = BO.getBusComp("State Model - State");
                    SiebelBusComp BCCHILD1 = BO.getBusComp("State Model - Transition");
                    SiebelBusComp BCCHILD2 = BO.getBusComp("Position");
                    SiebelBusComp BCPos = BOEng.getBusComp("State Model - Position");
                    SiebelBusComp oBCPick = new SiebelBusComp();

                    BC.activateField("Name");
                    BC.activateField("BusComp Name");
                    BC.activateField("Field Name");
                    BC.activateField("Activation Date/Time");
                    BC.activateField("Expiration Date/Time");
                    BC.activateField("Description");
                    BC.clearToQuery();
                    BC.setSearchExpr("[Name] ='" + sAdminC.getNombre() + "' ");
                    BC.executeQuery(true);
                    boolean reg = BC.firstRecord();
                    if (reg) {
                        if (sAdminC.getBusinessComponent() != null) {
                            if (!BC.getFieldValue("BusComp Name").equals(sAdminC.getBusinessComponent())) {
                                oBCPick = BC.getPicklistBusComp("BusComp Name");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchSpec("Name", sAdminC.getBusinessComponent());
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                    BC.writeRecord();
                                }
                            }
                        }

                        if (sAdminC.getCampo() != null) {
                            if (!BC.getFieldValue("Field Name").equals(sAdminC.getCampo())) {
                                oBCPick = BC.getPicklistBusComp("Field Name");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchSpec("Parent", sAdminC.getBusinessComponent());
                                oBCPick.setSearchSpec("Name", sAdminC.getCampo());
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getActivacion() != null) {
                            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                            String dateString = format.format(sAdminC.getActivacion());
                            if (!BC.getFieldValue("Activation Date/Time").equals(dateString)) {
                                BC.setFieldValue("Activation Date/Time", dateString);
                            }
                        }

                        if (sAdminC.getVencimiento() != null) {
                            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                            String dateString = format.format(sAdminC.getVencimiento());
                            if (!BC.getFieldValue("Expiration Date/Time").equals(dateString)) {
                                BC.setFieldValue("Expiration Date/Time", dateString);
                            }
                        }

                        if (sAdminC.getComentarios() != null) {
                            if (!BC.getFieldValue("Description").equals(sAdminC.getComentarios())) {
                                BC.setFieldValue("Description", sAdminC.getComentarios());
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Modelos de Estado:  " + sAdminC.getNombre());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Modelos de Estado";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowId(), MsgSalida, "Y", MsgError, usuario, version, ambienteInser, ambienteExtra);

//                    String RowId1 = BC.getFieldValue("Id");
//                    this.cargaBC(BCCHILD, oBCPick, RowId1, sAdminC.getRowId(),usuario,version,ambienteInser,ambienteExtra);    // SETEA REGIONES
//                    this.cargaBC2(BCCHILD1, BCCHILD2, BCPos, oBCPick, RowId1, sAdminC.getRowId(),usuario,version,ambienteInser,ambienteExtra);    // SETEA TRANSICIONES
                        BCPos.release();
                        BCCHILD2.release();
                        BCCHILD1.release();
                        BC.release();
                        BOEng.release();
                        BO.release();

                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getNombre() != null) {
                            BC.setFieldValue("Name", sAdminC.getNombre());
                        }

                        if (sAdminC.getBusinessComponent() != null) {
                            oBCPick = BC.getPicklistBusComp("BusComp Name");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchSpec("Name", sAdminC.getBusinessComponent());
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getCampo() != null) {
                            oBCPick = BC.getPicklistBusComp("Field Name");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchSpec("Parent Name", sAdminC.getBusinessComponent());
                            oBCPick.setSearchSpec("Name", sAdminC.getCampo());
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
                            BC.setFieldValue("Description", sAdminC.getComentarios());
                        }

                        BC.writeRecord();

                        System.out.println("Se crea registro de Modelos de Estado: " + sAdminC.getNombre());
                        String mensaje = ("Se crea registro de Modelos de Estado: " + sAdminC.getNombre());

                        String MsgSalida = "Creado Accion Polizas";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowId(), MsgSalida, "Y", MsgError, usuario, version, ambienteInser, ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

//                    String RowID = BC.getFieldValue("Id");
//                    this.cargaBC(BCCHILD, oBCPick, RowID, sAdminC.getRowId(),usuario,version,ambienteInser,ambienteExtra);    // SETEA REGIONES
//                    this.cargaBC2(BCCHILD1, BCCHILD2, BCPos, oBCPick, RowID, sAdminC.getRowId(),usuario,version,ambienteInser,ambienteExtra);    // SETEA TRANSICIONES
                        BCPos.release();
                        BCCHILD2.release();
                        BCCHILD1.release();
                        BC.release();
                        BOEng.release();
                        BO.release();

                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en creacion Modelos de Estado:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Modelos de Estado";
                    String FlagCarga = "E";
                    String MsgError = "Error en creacion Modelos de Estado:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " ");

                    this.CargaBitacoraSalidaError(sAdminC.getRowId(), MsgSalida, FlagCarga, MsgError, usuario, version, ambienteInser, ambienteExtra);
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

        try {                                                                       // BUSCA REGISTROS NUEVOS O ACTUALIZADOS, DESDE EL HIJO 1 - REGIONES
            List<ModelosEstadostransPuestoTrabajoSoloHijo1> hijo = new LinkedList();
            hijo = this.consultaSoloHijo1(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra);

            Boolean conteosolohijo = hijo.isEmpty(); // VALIDA SI LA LISTA ESTA VACIA
            if (!conteosolohijo) {

                ConexionSiebel conSiebel = new ConexionSiebel();
                SiebelDataBean m_dataBean = new SiebelDataBean();
                conSiebel.ConexionSiebel(m_dataBean, "SI", url, usuarioconn, passw);

                String IdPadre = null;

                for (ModelosEstadostransPuestoTrabajoSoloHijo1 sAdminC : hijo) {  // SI LA LISTA NO ESTA VACIA, COMIENZA EJECUCION
                    if ("SI".equals(Conectar)) {
                        conSiebel.ConexionSiebel(m_dataBean, Conectar, url, usuarioconn, passw);
                        Conectar = "NO";
                    }
                    try {
                        SiebelBusObject BO = m_dataBean.getBusObject("State Model");
                        SiebelBusComp BC = BO.getBusComp("State Model");
                        SiebelBusComp BCCHILD = BO.getBusComp("State Model - State");
                        SiebelBusComp oBCPick = new SiebelBusComp();

                        BC.activateField("Name");
                        BC.clearToQuery();
                        BC.setSearchExpr("[Name] ='" + sAdminC.getNombremodelo() + "' ");  // BUSCA PADRE PARA OBTENER ROW_ID DEL AMBIENTE DESTINO
                        BC.executeQuery(true);
                        boolean reg = BC.firstRecord();
                        if (reg) {
                            IdPadre = BC.getFieldValue("Id");  // OBTIENE ROW_ID DEL AMBIENTE DESTINO

                            // SI SE ENCONTRO ROW_ID DE PADRE, COMIENZA BUSQUEDA DE HIJOS 1 - CONDICIONES
                            BCCHILD.activateField("State Name");
                            BCCHILD.activateField("Restrict Delete Flag");
                            BCCHILD.activateField("Restrict Update Flag");
                            BCCHILD.activateField("Restrict Transition Flag");
                            BCCHILD.activateField("Description");
                            BCCHILD.activateField("State Model Id"); // Id de Padre
                            BCCHILD.clearToQuery();
                            BCCHILD.setSearchExpr("[State Name]='" + sAdminC.getNombreestado() + "' AND [State Model Id]='" + IdPadre + "'");
                            BCCHILD.executeQuery(true);
                            boolean regchild = BCCHILD.firstRecord();
                            if (regchild) {
                                if (sAdminC.getSineliminar() != null) {
                                    if (!BCCHILD.getFieldValue("Restrict Delete Flag").equals(sAdminC.getSineliminar())) {
                                        BCCHILD.setFieldValue("Restrict Delete Flag", sAdminC.getSineliminar());
                                    }
                                }
                                if (sAdminC.getSinactualizar() != null) {
                                    if (!BCCHILD.getFieldValue("Restrict Update Flag").equals(sAdminC.getSinactualizar())) {
                                        BCCHILD.setFieldValue("Restrict Update Flag", sAdminC.getSinactualizar());
                                    }
                                }
                                if (sAdminC.getRestringirtrans() != null) {
                                    if (!BCCHILD.getFieldValue("Restrict Transition Flag").equals(sAdminC.getRestringirtrans())) {
                                        BCCHILD.setFieldValue("Restrict Transition Flag", sAdminC.getRestringirtrans());
                                    }
                                }
                                if (sAdminC.getDescripcion() != null) {
                                    if (!BCCHILD.getFieldValue("Description").equals(sAdminC.getDescripcion())) {
                                        BCCHILD.setFieldValue("Description", sAdminC.getDescripcion());
                                    }
                                }

                                BCCHILD.writeRecord();

                                System.out.println("Se valida existencia y/o actualizacion de Regiones :  " + sAdminC.getNombreestado());
                                String MsgSalida = "Se valida existencia y/o actualizacion de Regiones";
                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(MsgSalida);

                                this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "", usuario, version, ambienteInser, ambienteExtra);

                                BCCHILD.release();
                                BC.release();
                                BO.release();

                            } else {
                                BCCHILD.newRecord(0);

                                if (sAdminC.getSineliminar() != null) {
                                    BCCHILD.setFieldValue("State Name", sAdminC.getNombreestado());
                                }

                                if (sAdminC.getSineliminar() != null) {
                                    BCCHILD.setFieldValue("Restrict Delete Flag", sAdminC.getSineliminar());
                                }

                                if (sAdminC.getSinactualizar() != null) {
                                    BCCHILD.setFieldValue("Restrict Update Flag", sAdminC.getSinactualizar());
                                }

                                if (sAdminC.getRestringirtrans() != null) {
                                    BCCHILD.setFieldValue("Restrict Transition Flag", sAdminC.getRestringirtrans());
                                }

                                if (sAdminC.getDescripcion() != null) {
                                    BCCHILD.setFieldValue("Description", sAdminC.getDescripcion());
                                }

                                BCCHILD.setFieldValue("State Model Id", IdPadre);
                                BCCHILD.writeRecord();

                                System.out.println("Se creo Solo Hijo - Region:  " + sAdminC.getNombreestado());
                                String mensaje = ("Se creo Solo Hijo - Region:  " + sAdminC.getNombreestado());

                                String MsgSalida = "Creado Solo Hijo - Region";
                                this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "", usuario, version, ambienteInser, ambienteExtra);

                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(mensaje);

                                BCCHILD.release();
                                BC.release();
                                BO.release();
                            }

                        } else {
                            System.out.println("No se encontro registro de Modelo de Estado: " + sAdminC.getNombremodelo() + " , en el ambiente a insertar, por esta razon no puede validarse si existen REGIONES a modificar o crear. ");
                            String MsgSalida = "No se encontro registro de Modelo de Estado: " + sAdminC.getNombremodelo() + " , en el ambiente a insertar, por esta razon no puede validarse si existen REGIONES a modificar o crear. ";
                            String FlagCarga = "E";
                            String MsgError = "No se encontro registro de Modelo de Estado: " + sAdminC.getNombremodelo() + " , en el ambiente a insertar, por esta razon no puede validarse si existen REGIONES a modificar o crear. ";

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgError);
                            this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError, usuario, version, ambienteInser, ambienteExtra);

                            BCCHILD.release();
                            BC.release();
                            BO.release();

                        }

                    } catch (SiebelException e) {
                        String error = e.getErrorMessage();
                        System.out.println("Error en creacion de Hijos - Region de Modelo de Estados:  " + sAdminC.getNombreestado() + "     , con el mensaje:   " + error.replace("'", " "));
                        String MsgSalida = "Error al Crear Hijos - Region de Modelo de Estados";
                        String FlagCarga = "E";
                        String MsgError = "Error en creacion de Hijos - Region de Modelo de Estados:  " + sAdminC.getNombreestado() + "     , con el mensaje:   " + error.replace("'", " ");

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgError);

                        this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError, usuario, version, ambienteInser, ambienteExtra);

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
            System.out.println("Error en  validacion Solo Hijo - Region de Modelo de Estados, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en  validacion Solo Hijo - Region de Modelo de Estados, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);

        }

        try {                                                                       // BUSCA REGISTROS NUEVOS O ACTUALIZADOS, DESDE EL HIJO 2 - TRANSICIONES
            List<ModelosEstadostransPuestoTrabajoSoloHijo2> hijo = new LinkedList();
            hijo = this.consultaSoloHijo2(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra);

            Boolean conteosolohijo = hijo.isEmpty(); // VALIDA SI LA LISTA ESTA VACIA
            if (!conteosolohijo) {

                ConexionSiebel conSiebel = new ConexionSiebel();
                SiebelDataBean m_dataBean = new SiebelDataBean();
                conSiebel.ConexionSiebel(m_dataBean, "SI", url, usuarioconn, passw);

                String NombrePadre = "";
                String IdPadre = null;

                for (ModelosEstadostransPuestoTrabajoSoloHijo2 sAdminC : hijo) {  // SI LA LISTA NO ESTA VACIA, COMIENZA EJECUCION
                    if ("SI".equals(Conectar)) {
                        conSiebel.ConexionSiebel(m_dataBean, Conectar, url, usuarioconn, passw);
                        Conectar = "NO";
                    }
                    try {
                        SiebelBusObject BO = m_dataBean.getBusObject("State Model");
                        SiebelBusComp BC = BO.getBusComp("State Model");
                        SiebelBusComp BCCHILD = BO.getBusComp("State Model - Transition");
                        SiebelBusComp oBCPick = new SiebelBusComp();

                        BC.activateField("Name");
                        BC.clearToQuery();
                        BC.setSearchExpr("[Name] ='" + sAdminC.getNombremodelo() + "' ");  // BUSCA PADRE PARA OBTENER ROW_ID DEL AMBIENTE DESTINO
                        BC.executeQuery(true);
                        boolean reg = BC.firstRecord();
                        if (reg) {
                            IdPadre = BC.getFieldValue("Id");  // OBTIENE ROW_ID DEL AMBIENTE DESTINO
                            
                            // SI SE ENCONTRO ROW_ID DE PADRE, COMIENZA BUSQUEDA DE HIJOS 1 - CONDICIONES
                            
                            BCCHILD.activateField("From State Name");
                            BCCHILD.activateField("To State Name");
                            BCCHILD.activateField("Public Flag");
                            BCCHILD.activateField("State Model Id"); // Id de Padre
                            BCCHILD.clearToQuery();
                            BCCHILD.setSearchExpr("[From State Name] ='" + sAdminC.getRegionOrigen() + "' AND [To State Name] ='" + sAdminC.getRegionDestino() + "' AND [State Model Id] ='" + IdPadre + "'");
                            BCCHILD.executeQuery(true);
                            boolean regchild = BCCHILD.firstRecord();
                            if (regchild) {
                                if (sAdminC.getPublico() != null) {
                                    if (!BCCHILD.getFieldValue("Public Flag").equals(sAdminC.getPublico())) {
                                        BCCHILD.setFieldValue("Public Flag", sAdminC.getPublico());
                                    }
                                }

                                BCCHILD.writeRecord();

                                System.out.println("Se valida existencia y/o actualizacion de Hijo Transiciones :  " + sAdminC.getRegionOrigen());
                                String MsgSalida = "Se valida existencia y/o actualizacion de Hijo Transiciones";
                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(MsgSalida);

                                this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "", usuario, version, ambienteInser, ambienteExtra);

                            } else {
                                BCCHILD.newRecord(0);

                                if (sAdminC.getRegionOrigen() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("From State Name");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[State Name]='" + sAdminC.getRegionOrigen() + "' ");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getRegionDestino() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("To State Name");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[State Name]='" + sAdminC.getRegionDestino() + "' ");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getPublico() != null) {
                                    BCCHILD.setFieldValue("Public Flag", sAdminC.getPublico());
                                }

                                BCCHILD.setFieldValue("State Model Id", IdPadre);
                                BCCHILD.writeRecord();

                                System.out.println("Se creo Hijo Transicion:  " + sAdminC.getRegionOrigen());
                                String mensaje = ("Se creo Hijo Transicion:  " + sAdminC.getRegionOrigen());

                                String MsgSalida = "Creado Hijo Transicion";
                                this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "", usuario, version, ambienteInser, ambienteExtra);

                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(mensaje);

                            }

                        } else {
                            System.out.println("No se encontro registro de Modelo de Estado: " + sAdminC.getNombremodelo() + " , en el ambiente a insertar, por esta razon no puede validarse si existen TRANSICIONES a modificar o crear. ");
                            String MsgSalida = "No se encontro registro de Modelo de Estado: " + sAdminC.getNombremodelo() + " , en el ambiente a insertar, por esta razon no puede validarse si existen TRANSICIONES a modificar o crear. ";
                            String FlagCarga = "E";
                            String MsgError = "No se encontro registro de Modelo de Estado: " + sAdminC.getNombremodelo() + " , en el ambiente a insertar, por esta razon no puede validarse si existen TRANSICIONES a modificar o crear. ";

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgError);
                            this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError, usuario, version, ambienteInser, ambienteExtra);

                            BCCHILD.release();
                            BC.release();
                            BO.release();

                        }

                    } catch (SiebelException e) {
                        String error = e.getErrorMessage();
                        System.out.println("Error en creacion de Hijo - Transicion de Modelo de Estados:  " + sAdminC.getRegionOrigen() + "     , con el mensaje:   " + error.replace("'", " "));
                        String MsgSalida = "Error al Crear Hijo - Transicion de Modelo de Estados";
                        String FlagCarga = "E";
                        String MsgError = "Error en creacion de Hijo - Transicion de Modelo de Estados:  " + sAdminC.getRegionOrigen() + "     , con el mensaje:   " + error.replace("'", " ");

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgError);

                        this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError, usuario, version, ambienteInser, ambienteExtra);

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
            System.out.println("Error en  validacion Solo Hijo - Transicion de Modelo de Estados, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en  validacion Solo Hijo - Transicion de Modelo de Estados, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);

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
    public List<ModelosEstadostransPuestoTrabajoPadre> consultaPadre(String fechaI, String fechaT, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.NAME \"Nombre\", A.BUSCOMP_NAME \"Business Component\", A.FIELD_NAME \"Campo\", A.ACTIVATE_DT \"Activación\", A.EXPIRE_DT \"Vencimiento\", A.DESC_TEXT \"Comentarios\"\n"
                + "FROM SIEBEL.S_STATE_MODEL A, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<ModelosEstadostransPuestoTrabajoPadre> modelosEstadostransPuestoTrabajo = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Padre a procesar.");

            while (rs.next()) {
                ModelosEstadostransPuestoTrabajoPadre lista = new ModelosEstadostransPuestoTrabajoPadre();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setBusinessComponent(rs.getString("Business Component"));
                lista.setCampo(rs.getString("Campo"));
                lista.setActivacion(rs.getDate("Activación"));
                lista.setVencimiento(rs.getDate("Vencimiento"));
                lista.setComentarios(rs.getString("Comentarios"));
                modelosEstadostransPuestoTrabajo.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre"), rs.getString("Business Component"), rs.getString("Campo"), "SE OBTIENEN DATOS", "MODELOS DE ESTADO", usuario, version, ambienteInser, ambienteExtra);

            }
            int Registros = modelosEstadostransPuestoTrabajo.size();
            Boolean conteo = modelosEstadostransPuestoTrabajo.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Padres de Modelos de Estado o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Padres de Modelos de Estado o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Padres de Modelos de Estado, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Padres de Modelos de Estado, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return modelosEstadostransPuestoTrabajo;
    }

    @Override
    public List<ModelosEstadostransPuestoTrabajoHijo1> consultaHijo1(String IdPadre, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.VALUE \"Nombre Estado\", B.RSTRCT_DEL_FLG \"Sin Eliminar\",B.RSTRCT_UPD_FLG \"Sin Actualizar\",B.RSTRCT_TRNS_FLG \"Restringir Trans\",B.DESC_TEXT \"Descripcion\"\n"
                + "FROM SIEBEL.S_SM_STATE B, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = B.LAST_UPD_BY\n"
                + "AND B.STATE_MODEL_ID = '" + IdPadre + "' AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')"
                + "ORDER BY B.LAST_UPD ASC";
        List<ModelosEstadostransPuestoTrabajoHijo1> modelosdeestado = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar - Regiones");

            while (rs.next()) {
                ModelosEstadostransPuestoTrabajoHijo1 lista = new ModelosEstadostransPuestoTrabajoHijo1();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setNombreestado(rs.getString("Nombre Estado"));
                lista.setSineliminar(rs.getString("Sin Eliminar"));
                lista.setSinactualizar(rs.getString("Sin Actualizar"));
                lista.setRestringirtrans(rs.getString("Restringir Trans"));
                lista.setDescripcion(rs.getString("Descripcion"));
                modelosdeestado.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre Estado"), "", "", "SE OBTIENEN DATOS - REGIONES", "MODELOS DE ESTADO - REGIONES", usuario, version, ambienteInser, ambienteExtra);

            }
            int Registros = modelosdeestado.size();
            Boolean conteo = modelosdeestado.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Regiones o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Regiones o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Regiones, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Regiones, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo1, con el error:  " + ex);
            throw ex;
        }
        return modelosdeestado;
    }

    @Override
    public List<ModelosEstadostransPuestoTrabajoSoloHijo1> consultaSoloHijo1(String fechaI, String fechaT, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.STATE_MODEL_ID \"Id Padre\", B.VALUE \"Nombre Estado\", B.RSTRCT_DEL_FLG \"Sin Eliminar\",B.RSTRCT_UPD_FLG \"Sin Actualizar\",B.RSTRCT_TRNS_FLG \"Restringir Trans\",B.DESC_TEXT \"Descripcion\",\n"
                + "A.NAME \"Nombre de Modelo\"\n"
                + "FROM SIEBEL.S_SM_STATE B, SIEBEL.S_STATE_MODEL A, SIEBEL.S_USER H\n"
                + "WHERE B.STATE_MODEL_ID = A.ROW_ID AND H.ROW_ID = B.LAST_UPD_BY\n"
                + "AND B.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND  H.LOGIN = '" + usuario + "' AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY B.LAST_UPD ASC";
        List<ModelosEstadostransPuestoTrabajoSoloHijo1> modelosdeestadosolohijo1 = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar - Regiones");

            while (rs.next()) {
                ModelosEstadostransPuestoTrabajoSoloHijo1 lista = new ModelosEstadostransPuestoTrabajoSoloHijo1();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setNombreestado(rs.getString("Nombre Estado"));
                lista.setSineliminar(rs.getString("Sin Eliminar"));
                lista.setSinactualizar(rs.getString("Sin Actualizar"));
                lista.setRestringirtrans(rs.getString("Restringir Trans"));
                lista.setDescripcion(rs.getString("Descripcion"));
                lista.setNombremodelo(rs.getString("Nombre de Modelo"));
                modelosdeestadosolohijo1.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre Estado"), "", "", "SE OBTIENEN DATOS - REGIONES", "MODELOS DE ESTADO - REGIONES", usuario, version, ambienteInser, ambienteExtra);

            }
            int Registros = modelosdeestadosolohijo1.size();
            Boolean conteo = modelosdeestadosolohijo1.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Regiones o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Regiones o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Regiones, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Regiones, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaSoloHijo1, con el error:  " + ex);
            throw ex;
        }
        return modelosdeestadosolohijo1;
    }

    @Override
    public List<ModelosEstadostransPuestoTrabajoHijo2> consultaHijo2(String IdPadre, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.STATE_MODEL_ID \"Id Padre\", C.VALUE \"Region Origen\", D.VALUE \"Region Destino\", B.PUBLIC_FLG \"Público\"\n"
                + "FROM SIEBEL.S_SM_TRANSITION B, SIEBEL.S_SM_STATE C, SIEBEL.S_SM_STATE D, SIEBEL.S_USER H\n"
                + "WHERE B.FROM_STATE_ID = C.ROW_ID AND B.TO_STATE_ID = D.ROW_ID AND H.ROW_ID = B.LAST_UPD_BY\n"
                + "AND B.STATE_MODEL_ID = '" + IdPadre + "'\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA E WHERE E.ROW_ID = B.ROW_ID AND E.USUARIO ='" + usuario + "' AND E.VERSION ='" + version + "' AND E.EXTRAER ='" + ambienteExtra + "' AND E.INSERTAR ='" + ambienteInser + "')"
                + "ORDER BY B.LAST_UPD ASC";
        List<ModelosEstadostransPuestoTrabajoHijo2> modelosdeestado = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar - Transiciones");

            while (rs.next()) {
                ModelosEstadostransPuestoTrabajoHijo2 lista = new ModelosEstadostransPuestoTrabajoHijo2();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setRegionOrigen(rs.getString("Region Origen"));
                lista.setRegionDestino(rs.getString("Region Destino"));
                lista.setPublico(rs.getString("Público"));
                modelosdeestado.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Region Origen"), rs.getString("Region Destino"), "", "SE OBTIENEN DATOS - TRANSICIONES", "MODELOS DE ESTADO - TRANSICIONES", usuario, version, ambienteInser, ambienteExtra);

            }
            int Registros = modelosdeestado.size();
            Boolean conteo = modelosdeestado.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Transiciones o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Transiciones o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Transiciones, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Transiciones, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo2, con el error:  " + ex);
            throw ex;
        }
        return modelosdeestado;
    }

    @Override
    public List<ModelosEstadostransPuestoTrabajoSoloHijo2> consultaSoloHijo2(String fechaI, String fechaT, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.STATE_MODEL_ID \"Id Padre\", C.VALUE \"Region Origen\", G.VALUE \"Region Destino\", B.PUBLIC_FLG \"Público\",A.NAME \"Nombre de Modelo\"\n"
                + "FROM SIEBEL.S_SM_TRANSITION B, SIEBEL.S_SM_STATE C, SIEBEL.S_SM_STATE G, SIEBEL.S_STATE_MODEL A, SIEBEL.S_USER H\n"
                + "WHERE B.FROM_STATE_ID = C.ROW_ID AND B.TO_STATE_ID = G.ROW_ID  AND B.STATE_MODEL_ID  = A.ROW_ID AND H.ROW_ID = B.LAST_UPD_BY\n"
                + "AND B.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND  H.LOGIN = '" + usuario + "' AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY B.LAST_UPD ASC";
        List<ModelosEstadostransPuestoTrabajoSoloHijo2> modelosdeestadosolohijo2 = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar - Transiciones");

            while (rs.next()) {
                ModelosEstadostransPuestoTrabajoSoloHijo2 lista = new ModelosEstadostransPuestoTrabajoSoloHijo2();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setRegionOrigen(rs.getString("Region Origen"));
                lista.setRegionDestino(rs.getString("Region Destino"));
                lista.setPublico(rs.getString("Público"));
                lista.setNombremodelo(rs.getString("Nombre de Modelo"));
                modelosdeestadosolohijo2.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Region Origen"), rs.getString("Region Destino"), "", "SE OBTIENEN DATOS - TRANSICIONES", "MODELOS DE ESTADO - TRANSICIONES", usuario, version, ambienteInser, ambienteExtra);

            }
            int Registros = modelosdeestadosolohijo2.size();
            Boolean conteo = modelosdeestadosolohijo2.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Transiciones o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Transiciones o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Transiciones, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Transiciones, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaSoloHijo2, con el error:  " + ex);
            throw ex;
        }
        return modelosdeestadosolohijo2;
    }

    @Override
    public void cargaBC2(SiebelBusComp BC, SiebelBusComp BC2, SiebelBusComp BC3, SiebelBusComp oBCPick, String RowID, String IdPadre, String usuario, String version, String ambienteInser, String ambienteExtra) throws SiebelException {
        try {
            List<ModelosEstadostransPuestoTrabajoHijo2> hijo = new LinkedList();
            hijo = this.consultaHijo2(IdPadre, usuario, version, ambienteInser, ambienteExtra);
            for (ModelosEstadostransPuestoTrabajoHijo2 sAdminC : hijo) {
                try {
                    BC.activateField("From State Name");
                    BC.activateField("To State Name");
                    BC.activateField("Public Flag");
                    BC.activateField("State Model Id"); // Id de Padre
                    BC.clearToQuery();
                    BC.setSearchSpec("From State Name", sAdminC.getRegionOrigen());
                    BC.setSearchSpec("To State Name", sAdminC.getRegionDestino());
                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {
                        if (sAdminC.getPublico() != null) {
                            if (!BC.getFieldValue("Public Flag").equals(sAdminC.getPublico())) {
                                BC.setFieldValue("Public Flag", sAdminC.getPublico());
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Transiciones :  " + sAdminC.getRegionOrigen());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Transiciones";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "", usuario, version, ambienteInser, ambienteExtra);

                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getRegionOrigen() != null) {
                            oBCPick = BC.getPicklistBusComp("From State Name");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[State Name]='" + sAdminC.getRegionOrigen() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getRegionDestino() != null) {
                            oBCPick = BC.getPicklistBusComp("To State Name");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[State Name]='" + sAdminC.getRegionDestino() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getPublico() != null) {
                            BC.setFieldValue("Public Flag", sAdminC.getPublico());
                        }

                        BC.setFieldValue("State Model Id", RowID);
                        BC.writeRecord();

                        System.out.println("Se creo Transicion:  " + sAdminC.getRegionOrigen());
                        String mensaje = ("Se creo Transicion:  " + sAdminC.getRegionOrigen());

                        String MsgSalida = "Creado Transicion";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "", usuario, version, ambienteInser, ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

                    }

                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en Transicion:" + sAdminC.getRegionOrigen() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Hijo2-Modelos de Estado";
                    String FlagCarga = "E";
                    String MsgError = "Error en Transicion:" + sAdminC.getRegionOrigen() + "     , con el mensaje:   " + error.replace("'", " ");

                    this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError, usuario, version, ambienteInser, ambienteExtra);

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
            Logger.getLogger(DAOConjuntoReglasValidacionImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

//    @Override
//    public void cargaBC3(SiebelBusComp BC, SiebelBusComp BC2, SiebelBusComp oBCPick, String RowID, String IdPadre) throws SiebelException {
//        try {
//            List<ModelosEstadostransPuestoTrabajoHijo3> hijo = new LinkedList();
//            hijo = this.consultaHijo3(IdPadre);
//            //this.ConectarDB();
//            for (ModelosEstadostransPuestoTrabajoHijo3 sAdminC : hijo) {
//                try {
//                    BC.activateField("Name");
//                    BC.activateField("Division");
//                    BC.activateField("Position Type");
//                    //BC.activateField("Action Id"); // Id de Padre
//                    BC.clearToQuery();
//                    BC.setSearchSpec("Name", sAdminC.getPuestoTrabajo());
//                    BC.executeQuery(true);
//                    boolean regchild = BC.firstRecord();
//                    if (regchild) {
//                        if (sAdminC.getDivicion() != null) {
//                            if (!BC.getFieldValue("Division").equals(sAdminC.getDivicion())) {
//                                oBCPick = BC.getPicklistBusComp("Division");
//                                oBCPick.clearToQuery();
//                                oBCPick.setSearchSpec("Name", sAdminC.getDivicion());
//                                oBCPick.executeQuery(true);
//                                if (oBCPick.firstRecord()) {
//                                    oBCPick.pick();
//                                    BC.writeRecord();
//                                }
//                                oBCPick.release();
//                            }
//                        }
//
//                        if (sAdminC.getTipoPuestoTrabajo() != null) {
//                            if (!BC.getFieldValue("Position Type").equals(sAdminC.getTipoPuestoTrabajo())) {
//                                oBCPick = BC.getPicklistBusComp("Position Type");
//                                oBCPick.clearToQuery();
//                                oBCPick.setSearchSpec("Value", sAdminC.getTipoPuestoTrabajo());
//                                oBCPick.executeQuery(true);
//                                if (oBCPick.firstRecord()) {
//                                    oBCPick.pick();
//                                    BC.writeRecord();
//                                }
//                                oBCPick.release();
//                            }
//                        }
//                    } else {
//                        BC.newRecord(0);
//
//                        if (sAdminC.getPuestoTrabajo() != null) {
//                            BC.setFieldValue("Name", sAdminC.getPuestoTrabajo());
//                        }
//
//                        if (sAdminC.getDivicion() != null) {
//                            oBCPick = BC.getPicklistBusComp("Division");
//                            oBCPick.clearToQuery();
//                            oBCPick.setSearchSpec("Name", sAdminC.getDivicion());
//                            oBCPick.executeQuery(true);
//                            if (oBCPick.firstRecord()) {
//                                oBCPick.pick();
//                                BC.writeRecord();
//                            }
//                            oBCPick.release();
//                        }
//
//                        if (sAdminC.getTipoPuestoTrabajo() != null) {
//                            oBCPick = BC.getPicklistBusComp("Position Type");
//                            oBCPick.clearToQuery();
//                            oBCPick.setSearchSpec("Value", sAdminC.getTipoPuestoTrabajo());
//                            oBCPick.executeQuery(true);
//                            if (oBCPick.firstRecord()) {
//                                oBCPick.pick();
//                                BC.writeRecord();
//                            }
//                            oBCPick.release();
//
//                        }
//
//                        //BC.setFieldValue("Action Id", RowID);
//                        BC.writeRecord();
//
//                        String DivId = BC.getFieldValue("Id");
//
//                        try {
//                            BC2.activateField("Transition Id");
//                            BC2.activateField("Position Id");
//                            BC2.clearToQuery();
//                            BC2.setSearchSpec("Transition Id", RowID);
//                            BC2.setSearchSpec("Position Id", DivId);
//                            BC2.executeQuery(true);
//                            boolean regchild2 = BC2.firstRecord();
//                            if (regchild2) {
//
//                                // SIN ACCION
//                            } else {
//                                BC2.newRecord(0);
//
//                                BC2.setFieldValue("Transition Id", RowID);
//                                BC2.setFieldValue("Position Id", DivId);
//
//                                BC2.writeRecord();
//
//                                System.out.println("Se creo registro en intermedia de posicion:   " + sAdminC.getPuestoTrabajo());
//                                String mensaje = ("Se creo registro en intermedia de posicion:   " + sAdminC.getPuestoTrabajo());
//                                Reportes rep = new Reportes();
//                                rep.agregarTextoAlfinal(mensaje);
//                            }
//
//                        } catch (SiebelException e) {
//                            String error = e.getErrorMessage();
//                            System.out.println("Error tabla intermedia posicion:   " + sAdminC.getPuestoTrabajo() + "     , con el mensaje:   " + error.replace("'", " "));
//                            String MsgSalida = "Error al Crear Hijo3-Modelos de Estado";
//                            String FlagCarga = "E";
//                            String MsgError = "Error tabla intermedia posicion:   " + sAdminC.getPuestoTrabajo() + "     , con el mensaje:   " + error.replace("'", " ");
//
//                            this.CargaBitacoraSalidaError(RowID, MsgSalida, FlagCarga, MsgError);
//
//                            Reportes rep = new Reportes();
//                            rep.agregarTextoAlfinal(MsgError);
//
//                        }
//
//                        System.out.println("Se creo Posicion:   " + sAdminC.getPuestoTrabajo());
//                        String mensaje = ("Se creo Posicion:   " + sAdminC.getPuestoTrabajo());
//                        Reportes rep = new Reportes();
//                        rep.agregarTextoAlfinal(mensaje);
//                    }
//                    //this.updateAdministracionMatrizDescuentos(sAdminC.getRowId());
//                } catch (SiebelException e) {
//                    String error = e.getErrorMessage();
//                    System.out.println("Error en Posicion:   " + sAdminC.getPuestoTrabajo() + "     , con el mensaje:   " + error.replace("'", " "));
//                    String MsgSalida = "Error al Crear Hijo3-Modelos de Estado";
//                    String FlagCarga = "E";
//                    String MsgError = "Error en Posicion:   " + sAdminC.getPuestoTrabajo() + "     , con el mensaje:   " + error.replace("'", " ");
//
//                    this.CargaBitacoraSalidaError(RowID, MsgSalida, FlagCarga, MsgError);
//
//                    Reportes rep = new Reportes();
//                    rep.agregarTextoAlfinal(MsgError);
//
//                }
//            }
//            //this.CloseDB();
//        } catch (Exception e) {
//            try {
//                throw e;
//            } catch (Exception ex) {
//                Logger.getLogger(DAOModelosEstadostransPuestoTrabajoImpl.class.getName()).log(Level.SEVERE, null, ex);
//            }
//        }
//    }
    @Override
    public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick, String RowID, String IdPadre, String usuario, String version, String ambienteInser, String ambienteExtra) throws SiebelException {
        try {
            List<ModelosEstadostransPuestoTrabajoHijo1> hijo = new LinkedList();
            hijo = this.consultaHijo1(IdPadre, usuario, version, ambienteInser, ambienteExtra);
            for (ModelosEstadostransPuestoTrabajoHijo1 sAdminC : hijo) {
                try {
                    BC.activateField("State Name");
                    BC.activateField("Restrict Delete Flag");
                    BC.activateField("Restrict Update Flag");
                    BC.activateField("Restrict Transition Flag");
                    BC.activateField("Description");
                    BC.activateField("State Model Id"); // Id de Padre
                    BC.clearToQuery();
                    BC.setSearchExpr("[State Name]='" + sAdminC.getNombreestado() + "' ");
                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {
                        if (sAdminC.getSineliminar() != null) {
                            if (!BC.getFieldValue("Restrict Delete Flag").equals(sAdminC.getSineliminar())) {
                                BC.setFieldValue("Restrict Delete Flag", sAdminC.getSineliminar());
                            }
                        }
                        if (sAdminC.getSinactualizar() != null) {
                            if (!BC.getFieldValue("Restrict Update Flag").equals(sAdminC.getSinactualizar())) {
                                BC.setFieldValue("Restrict Update Flag", sAdminC.getSinactualizar());
                            }
                        }
                        if (sAdminC.getRestringirtrans() != null) {
                            if (!BC.getFieldValue("Restrict Transition Flag").equals(sAdminC.getRestringirtrans())) {
                                BC.setFieldValue("Restrict Transition Flag", sAdminC.getRestringirtrans());
                            }
                        }
                        if (sAdminC.getDescripcion() != null) {
                            if (!BC.getFieldValue("Description").equals(sAdminC.getDescripcion())) {
                                BC.setFieldValue("Description", sAdminC.getDescripcion());
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Regiones :  " + sAdminC.getNombreestado());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Regiones";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "", usuario, version, ambienteInser, ambienteExtra);

                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getSineliminar() != null) {
                            BC.setFieldValue("State Name", sAdminC.getNombreestado());
                        }

                        if (sAdminC.getSineliminar() != null) {
                            BC.setFieldValue("Restrict Delete Flag", sAdminC.getSineliminar());
                        }

                        if (sAdminC.getSinactualizar() != null) {
                            BC.setFieldValue("Restrict Update Flag", sAdminC.getSinactualizar());
                        }

                        if (sAdminC.getRestringirtrans() != null) {
                            BC.setFieldValue("Restrict Transition Flag", sAdminC.getRestringirtrans());
                        }

                        if (sAdminC.getDescripcion() != null) {
                            BC.setFieldValue("Description", sAdminC.getDescripcion());
                        }

                        BC.setFieldValue("State Model Id", RowID);
                        BC.writeRecord();

                        System.out.println("Se creo Region:  " + sAdminC.getNombreestado());
                        String mensaje = ("Se creo Region:  " + sAdminC.getNombreestado());

                        String MsgSalida = "Creado Region";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "", usuario, version, ambienteInser, ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);
                    }

                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en Region:   " + sAdminC.getNombreestado() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Hijo-Modelos de Estado";
                    String FlagCarga = "E";
                    String MsgError = "Error en Region:   " + sAdminC.getNombreestado() + "     , con el mensaje:   " + error.replace("'", " ");

                    this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError, usuario, version, ambienteInser, ambienteExtra);

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
            Logger.getLogger(DAOModelosEstadostransPuestoTrabajoImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     *
     * @param IdPadre
     * @return
     * @throws SQLException
     */
//    @Override
//    public List<objetos.ModelosEstadostransPuestoTrabajoHijo3> consultaHijo3(String IdPadre) throws SQLException {
//
//        String readRecordSQL = "SELECT ROW_ID, POSITION_ID \"Id Puesto Trab\",  STATE_TRNS_ID \"Id Transicion\"\n"
//                + "FROM SIEBEL.S_SM_TRNS_POSTN\n"
//                + "WHERE STATE_TRANS_ID = '" + IdPadre + "'";
//        List<ModelosEstadostransPuestoTrabajoHijo3> conjuntoReglasValidacion = new LinkedList();
//        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {
//            while (rs.next()) {
//                ModelosEstadostransPuestoTrabajoHijo3 lista = new ModelosEstadostransPuestoTrabajoHijo3();
//                lista.setRowid(rs.getString("ROW_ID"));
//                lista.setPuestoTrabajo(rs.getString("Nombre Estado"));
//                lista.setPuestoTrabajo(rs.getString("Sin Eliminar"));
////                conjuntoReglasValidacion.add(lista);
//            }
//            System.out.println("Se obtiene Lista de Hijos: Transiciones de Administracion de Descuento");
//            String mensaje = "Se obtiene Lista de Hijos: Transiciones de Administracion de Descuento";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(mensaje);
//        } catch (SQLException ex) {
//            //Exception
//            throw ex;
//        }//finally{this.CloseDB();}
//        return conjuntoReglasValidacion;
//    }
    private void CargaBitacoraEntrada(String Row_Id, String Val1, String Val2, String Val3, String Seguimiento, String Objeto, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {

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

    private void CargaBitacoraSalidaCreado(String rowId, String MsgSalida, String FlagCarga, String MsgError, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {

        String readRecordSQL4 = "UPDATE SIEBEL.CT_BITACORA SET FLAG_CARGA = '" + FlagCarga + "', SEGUIMIENTO = '" + MsgSalida + "', MENSAJE_ERROR = '" + MsgError + "'\n"
                + "WHERE ROW_ID = '" + rowId + "' AND USUARIO = '" + usuario + "' AND VERSION = '" + version + "' AND EXTRAER = '" + ambienteExtra + "' AND INSERTAR = '" + ambienteInser + "' ";

        PreparedStatement ps3 = conexion.prepareStatement(readRecordSQL4);
        ps3.executeUpdate();
        ps3.close();
    }

    private void CargaBitacoraSalidaValidado(String rowId, String MsgSalida, String FlagCarga, String MsgError, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {

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

    private void CargaBitacoraSalidaError(String rowId, String MsgSalida, String FlagCarga, String MsgError, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {

        String readRecordSQL6 = "UPDATE SIEBEL.CT_BITACORA SET FLAG_CARGA = '" + FlagCarga + "', SEGUIMIENTO = '" + MsgSalida + "', MENSAJE_ERROR = '" + MsgError + "'\n"
                + "WHERE ROW_ID = '" + rowId + "' AND USUARIO = '" + usuario + "' AND VERSION = '" + version + "' AND EXTRAER = '" + ambienteExtra + "' AND INSERTAR = '" + ambienteInser + "'";

        PreparedStatement ps5 = conexion.prepareStatement(readRecordSQL6);
        ps5.executeUpdate();
        ps5.close();
    }
}
