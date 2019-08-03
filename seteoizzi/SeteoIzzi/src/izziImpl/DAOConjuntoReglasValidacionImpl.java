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
import interfaces.DAOConjuntoReglasValidacion;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import objetos.ConjuntoReglasValidacionHijo1;
import objetos.ConjuntoReglasValidacionHijo2;
import objetos.ConjuntoReglasValidacionPadre;
import objetos.ConjuntoReglasValidacionSoloHijo1;
import objetos.ConjuntoReglasValidacionSoloHijo2;

/**
 *
 * @author hector.pineda
 */
public class DAOConjuntoReglasValidacionImpl extends ConexionDB implements DAOConjuntoReglasValidacion {

    @Override
    public void inserta(List<ConjuntoReglasValidacionPadre> conjuntoReglasValidacion, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {


        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);
        
        Boolean conteopadre = conjuntoReglasValidacion.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

        for (ConjuntoReglasValidacionPadre sAdminC : conjuntoReglasValidacion) {
            if ("SI".equals(Conectar)) {
                conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                Conectar = "NO";
            }

            try {

                String MsgError = "";

                SiebelBusObject BO = m_dataBean.getBusObject("FINS Validation");
                SiebelBusComp BC = BO.getBusComp("FINS Validation Rule Set");
                SiebelBusComp BCCHILD = BO.getBusComp("FINS Validation Rule");
                SiebelBusComp BCCHILD2 = BO.getBusComp("FINS Validation Rule Set Arguments");
                SiebelBusComp oBCPick = new SiebelBusComp();

                BC.activateField("Name");
                BC.activateField("Call Name");
                BC.activateField("Business Component");
                BC.activateField("Business Object");
                BC.activateField("Status");
                BC.activateField("View");
                BC.activateField("Conditional Expr");
                BC.activateField("Start Date");
                BC.activateField("End Date");
                BC.activateField("Aggregate Error Flag");
                BC.activateField("Description");
                BC.clearToQuery();
                BC.setSearchExpr("[Name] ='" + sAdminC.getNombre() + "' ");
                BC.executeQuery(true);
                boolean reg = BC.firstRecord();
                if (reg) {

                    if (sAdminC.getGrupo() != null) {
                        if (!BC.getFieldValue("Call Name").equals(sAdminC.getGrupo())) {
                            BC.setFieldValue("Call Name", sAdminC.getGrupo());
                        }
                    }

                    if (sAdminC.getBusinessComponent() != null) {
                        if (!BC.getFieldValue("Business Component").equals(sAdminC.getBusinessComponent())) {
                            oBCPick = BC.getPicklistBusComp("Business Component");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name] ='" + sAdminC.getBusinessComponent()+ "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }
                    }

                    if (sAdminC.getEstado() != null) {
                        if (!BC.getFieldValue("Status").equals(sAdminC.getEstado())) {
                            oBCPick = BC.getPicklistBusComp("Status");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value] ='" + sAdminC.getEstado()+ "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }
                    }

                    if (sAdminC.getBusinessObject() != null) {
                        if (!BC.getFieldValue("Business Object").equals(sAdminC.getBusinessObject())) {
                            oBCPick = BC.getPicklistBusComp("Business Object");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name] ='" + sAdminC.getBusinessObject()+ "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }
                    }

                    if (sAdminC.getVer() != null) {
                        if (!BC.getFieldValue("View").equals(sAdminC.getVer())) {
                            oBCPick = BC.getPicklistBusComp("View");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name] ='" + sAdminC.getVer()+ "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }
                    }

                    if (sAdminC.getExpresionCondicional() != null) {
                        if (!BC.getFieldValue("Conditional Expr").equals(sAdminC.getExpresionCondicional())) {
                            BC.setFieldValue("Conditional Expr", sAdminC.getExpresionCondicional());
                        }
                    }

                    if (sAdminC.getFechaInicio() != null) {
                        if (!BC.getFieldValue("Start Date").equals(sAdminC.getFechaInicio())) {
                            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                            String dateString = format.format(sAdminC.getFechaInicio());
                            BC.setFieldValue("Start Date", dateString);
                        }
                    }

                    if (sAdminC.getFechaFinal() != null) {
                        if (!BC.getFieldValue("End Date").equals(sAdminC.getFechaFinal())) {
                            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                            String dateString = format.format(sAdminC.getFechaFinal());
                            BC.setFieldValue("End Date", dateString);
                        }
                    }

                    if (sAdminC.getAgregacionErrores() != null) {
                        if (!BC.getFieldValue("Aggregate Error Flag").equals(sAdminC.getAgregacionErrores())) {
                            BC.setFieldValue("Aggregate Error Flag", sAdminC.getAgregacionErrores());
                        }
                    }

                    if (sAdminC.getDescripcion() != null) {
                        if (!BC.getFieldValue("Description").equals(sAdminC.getDescripcion())) {
                            BC.setFieldValue("Description", sAdminC.getDescripcion());
                        }
                    }

                    BC.writeRecord();

                    System.out.println("Se valida existencia y/o actualizacion de Conjunto de Reglas de Validacion:  " + sAdminC.getNombre());
                    String MsgSalida = "Se valida existencia y/o actualizacion de Conjunto de Reglas de Validacion";

                    this.CargaBitacoraSalidaValidado(sAdminC.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgSalida);

//                    String RowId1 = BC.getFieldValue("Id");
//                    this.cargaBC(BCCHILD, oBCPick, RowId1, sAdminC.getRowId(),usuario,version,ambienteInser,ambienteExtra);  // SETEA REGLAS
//                    this.cargaBC2(BCCHILD2, oBCPick, RowId1, sAdminC.getRowId(),usuario,version,ambienteInser,ambienteExtra); // SETEA ARGUMENTOS

                    BCCHILD2.release();
                    BCCHILD.release();
                    BC.release();
                    BO.release();

                } else {
                    BC.newRecord(0);

                    if (sAdminC.getNombre() != null) {
                        BC.setFieldValue("Name", sAdminC.getNombre());
                    }

                    if (sAdminC.getGrupo() != null) {
                        BC.setFieldValue("Call Name", sAdminC.getGrupo());
                    }

                    if (sAdminC.getBusinessObject() != null) {
                        oBCPick = BC.getPicklistBusComp("Business Component");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Name] ='" + sAdminC.getBusinessObject()+ "'");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdminC.getVersion() != null) {
                        BC.setFieldValue("Version", sAdminC.getVersion());
                    }

                    if (sAdminC.getEstado() != null) {
                        oBCPick = BC.getPicklistBusComp("Status");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Value] ='" + sAdminC.getEstado()+ "'");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdminC.getVer() != null) {
                        oBCPick = BC.getPicklistBusComp("View");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Name] ='" + sAdminC.getVer()+ "'");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();
                    }

                    if (sAdminC.getExpresionCondicional() != null) {
                        BC.setFieldValue("Conditional Expr", sAdminC.getExpresionCondicional());
                    }

                    if (sAdminC.getFechaInicio() != null) {
                        SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                        String dateString = format.format(sAdminC.getFechaInicio());
                        BC.setFieldValue("Start Date", dateString);
                    }

                    if (sAdminC.getFechaFinal() != null) {
                        SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                        String dateString = format.format(sAdminC.getFechaFinal());
                        BC.setFieldValue("End Date", dateString);
                    }

                    if (sAdminC.getAgregacionErrores() != null) {
                        BC.setFieldValue("Aggregate Error Flag", sAdminC.getAgregacionErrores());
                    }

                    if (sAdminC.getDescripcion() != null) {
                        BC.setFieldValue("Description", sAdminC.getDescripcion());
                    }

                    BC.writeRecord();

                    System.out.println("Se crea registro de Conjunto de Reglas de Validacion: " + sAdminC.getNombre());
                    String mensaje = ("Se crea registro de Conjunto de Reglas de Validacion: " + sAdminC.getNombre());

                    String MsgSalida = "Creado Conjunto de Reglas de Validacion";
                    this.CargaBitacoraSalidaCreado(sAdminC.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(mensaje);

//                    String RowID = BC.getFieldValue("Id");
//                    this.cargaBC(BCCHILD, oBCPick, RowID, sAdminC.getRowId(),usuario,version,ambienteInser,ambienteExtra);   // SETEA REGLAS
//                    this.cargaBC2(BCCHILD2, oBCPick, RowID, sAdminC.getRowId(),usuario,version,ambienteInser,ambienteExtra);  // SETEA ARGUMENTOS

                    BCCHILD2.release();
                    BCCHILD.release();
                    BC.release();
                    BO.release();
                }
            } catch (SiebelException e) {
                String error = e.getErrorMessage();
                System.out.println("Error en creacion Conjunto de Reglas de Validacion:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " "));
                String MsgSalida = "Error al Crear Conjunto de Reglas de Validacion";
                String FlagCarga = "E";
                String MsgError = "Error en creacion Conjunto de Reglas de Validacion:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " ");

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
        
        
    try {                                                                       // BUSCA REGISTROS NUEVOS O ACTUALIZADOS, DESDE EL HIJO 1 - REGLAS
            List<ConjuntoReglasValidacionSoloHijo1> hijo = new LinkedList();
            hijo = this.consultaSoloHijo1(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra);

            Boolean conteosolohijo = hijo.isEmpty(); // VALIDA SI LA LISTA ESTA VACIA
            if (!conteosolohijo) {

                ConexionSiebel conSiebel = new ConexionSiebel();
                SiebelDataBean m_dataBean = new SiebelDataBean();
                conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

                for (ConjuntoReglasValidacionSoloHijo1 sAdminC : hijo) {  // SI LA LISTA NO ESTA VACIA, COMIENZA EJECUCION
                    if ("SI".equals(Conectar)) {
                        conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                        Conectar = "NO";
                    }
                    try {
                        SiebelBusObject BO = m_dataBean.getBusObject("FINS Validation");
                        SiebelBusComp BC = BO.getBusComp("FINS Validation Rule Set");
                        SiebelBusComp BCCHILD = BO.getBusComp("FINS Validation Rule");
                        SiebelBusComp oBCPick = new SiebelBusComp();

                        BC.activateField("Name");
                        BC.clearToQuery();
                        BC.setSearchExpr("[Name] ='" + sAdminC.getNombreconjuntoregla()+ "' ");  // BUSCA PADRE PARA OBTENER ROW_ID DEL AMBIENTE DESTINO
                        BC.executeQuery(true);
                        boolean reg = BC.firstRecord();
                        if (reg) {
                            String IdPadre = BC.getFieldValue("Id");  // OBTIENE ROW_ID DEL AMBIENTE DESTINO

                            // SI SE ENCONTRO ROW_ID DE PADRE, COMIENZA BUSQUEDA DE HIJOS 1 - CONDICIONES
                            BCCHILD.activateField("Sequence Num");
                            BCCHILD.activateField("Name");
                            BCCHILD.activateField("Expression");
                            BCCHILD.activateField("Business Component");
                            BCCHILD.activateField("Apply To Type");
                            BCCHILD.activateField("Start Date");
                            BCCHILD.activateField("End Date");
                            BCCHILD.activateField("Description");
                            BCCHILD.activateField("Return Code");
                            BCCHILD.activateField("Rule Set Id"); // Id de Padre
                            BCCHILD.clearToQuery();
                            BCCHILD.setSearchExpr("[Name]='" + sAdminC.getNombre() + "' AND [Rule Set Id]='" + IdPadre + "'");
                            BCCHILD.executeQuery(true);
                            boolean regchild = BCCHILD.firstRecord();
                            if (regchild) {
                                if (sAdminC.getNumSecuencia() != null) {
                                    if (!BCCHILD.getFieldValue("Sequence Num").equals(sAdminC.getNumSecuencia())) {
                                        BCCHILD.setFieldValue("Sequence Num", sAdminC.getNumSecuencia());
                                    }
                                }

                                if (sAdminC.getExprecion() != null) {
                                    if (!BCCHILD.getFieldValue("Expression").equals(sAdminC.getExprecion())) {
                                        BCCHILD.setFieldValue("Expression", sAdminC.getExprecion());
                                    }
                                }

                                if (sAdminC.getBusinessComponent() != null) {
                                    if (!BCCHILD.getFieldValue("Business Component").equals(sAdminC.getBusinessComponent())) {
                                        oBCPick = BCCHILD.getPicklistBusComp("Business Component");
                                        oBCPick.clearToQuery();
                                        oBCPick.setSearchSpec("Name", sAdminC.getBusinessComponent());
                                        oBCPick.executeQuery(true);
                                        if (oBCPick.firstRecord()) {
                                            oBCPick.pick();
                                        }
                                        oBCPick.release();
                                    }
                                }

                                if (sAdminC.getAplicar() != null) {
                                    if (!BCCHILD.getFieldValue("Apply To Type").equals(sAdminC.getAplicar())) {
                                        oBCPick = BCCHILD.getPicklistBusComp("Apply To Type");
                                        oBCPick.clearToQuery();
                                        oBCPick.setSearchSpec("Value", sAdminC.getAplicar());
                                        oBCPick.executeQuery(true);
                                        if (oBCPick.firstRecord()) {
                                            oBCPick.pick();
                                        }
                                        oBCPick.release();
                                    }
                                }

                                if (sAdminC.getFechaInicio() != null) {
                                    SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                                    String dateString = format.format(sAdminC.getFechaInicio());
                                    if (!BCCHILD.getFieldValue("Activation Date/Time").equals(dateString)) {
                                        BCCHILD.setFieldValue("Start Date", dateString);
                                    }
                                }

                                if (sAdminC.getFechaFinal() != null) {
                                    SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                                    String dateString = format.format(sAdminC.getFechaFinal());
                                    if (!BCCHILD.getFieldValue("Activation Date/Time").equals(dateString)) {
                                        BCCHILD.setFieldValue("End Date", dateString);
                                    }
                                }

                                if (sAdminC.getDescripcion() != null) {
                                    if (!BCCHILD.getFieldValue("Activation Date/Time").equals(sAdminC.getDescripcion())) {
                                        BCCHILD.setFieldValue("Description", sAdminC.getDescripcion());
                                    }
                                }

                                if (sAdminC.getAplicar() != null) {
                                    if (!BCCHILD.getFieldValue("Return Code").equals(sAdminC.getAplicar())) {
                                        oBCPick = BCCHILD.getPicklistBusComp("Return Code");
                                        oBCPick.clearToQuery();
                                        oBCPick.setSearchSpec("Message Code", sAdminC.getCodigoRetorno());
                                        oBCPick.executeQuery(true);
                                        if (oBCPick.firstRecord()) {
                                            oBCPick.pick();
                                        }
                                        oBCPick.release();
                                    }

                                    BCCHILD.writeRecord();

                                    System.out.println("Se valida existencia y/o actualizacion de Reglas :  " + sAdminC.getNombre());
                                    String MsgSalida = "Se valida existencia y/o actualizacion de Reglas";

                                    this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                    Reportes rep = new Reportes();
                                    rep.agregarTextoAlfinal(MsgSalida);
                                    

                                BCCHILD.release();
                                BC.release();
                                BO.release();

                                }
                            } else {
                                BCCHILD.newRecord(0);

                                if (sAdminC.getNumSecuencia() != null) {
                                    BCCHILD.setFieldValue("Sequence Num", sAdminC.getNumSecuencia());
                                }

                                if (sAdminC.getNombre() != null) {
                                    BCCHILD.setFieldValue("Name", sAdminC.getNombre());
                                }

                                if (sAdminC.getExprecion() != null) {
                                    BCCHILD.setFieldValue("Expression", sAdminC.getExprecion());
                                }

                                if (sAdminC.getBusinessComponent() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("Business Component");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchSpec("Name", sAdminC.getBusinessComponent());
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getAplicar() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("Apply To Type");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchSpec("Value", sAdminC.getAplicar());
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getFechaInicio() != null) {
                                    SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                                    String dateString = format.format(sAdminC.getFechaInicio());
                                    BCCHILD.setFieldValue("Start Date", dateString);
                                }

                                if (sAdminC.getFechaFinal() != null) {
                                    SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                                    String dateString = format.format(sAdminC.getFechaFinal());
                                    BCCHILD.setFieldValue("End Date", dateString);
                                }

                                if (sAdminC.getDescripcion() != null) {
                                    BCCHILD.setFieldValue("Description", sAdminC.getDescripcion());
                                }

                                if (sAdminC.getAplicar() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("Return Code");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchSpec("Message Code", sAdminC.getCodigoRetorno());
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                BCCHILD.setFieldValue("Rule Set Id", IdPadre);
                                BCCHILD.writeRecord();
                                System.out.println("Se creo registro de Regla:  " + sAdminC.getNombre());
                                String mensaje = ("Se creo registro de Regla:  " + sAdminC.getNombre());

                                String MsgSalida = "Creado registro de Regla";
                                this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(mensaje);
                                
                                BCCHILD.release();
                                BC.release();
                                BO.release();
                            }

                        } else {
                            System.out.println("No se encontro registro de Conjunto de Reglas de Validacion: " + sAdminC.getNombreconjuntoregla() + " , en el ambiente a insertar, por esta razon no puede validarse si existen REGLAS a modificar o crear. ");
                            String MsgSalida = "No se encontro registro de Conjunto de Reglas de Validacion en el ambiente a insertar";
                            String FlagCarga = "E";
                            String MsgError = "No se encontro registro de Conjunto de Reglas de Validacion con nombre: " + sAdminC.getNombreconjuntoregla() + " , en el ambiente a insertar, por esta razon no puede validarse si existen REGLAS a modificar o crear. ";

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgError);
                            this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteExtra,ambienteInser);

                            BCCHILD.release();
                            BC.release();
                            BO.release();
                        }

                    } catch (SiebelException e) {
                        String error = e.getErrorMessage();
                        System.out.println("Error en creacion de Hijos - REGLAS:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " "));
                        String MsgSalida = "Error al Crear Hijos - REGLAS";
                        String FlagCarga = "E";
                        String MsgError = "Error en creacion de Hijos - REGLAS:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " ");

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgError);

                        this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteExtra,ambienteInser);

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
            System.out.println("Error en  validacion Solo Hijo - Reglas, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en  validacion Solo Hijo - Reglas, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);

        }
    
    
    try {                                                                       // BUSCA REGISTROS NUEVOS O ACTUALIZADOS, DESDE EL HIJO 2 - ARGUMENTOS
            List<ConjuntoReglasValidacionSoloHijo2> hijo = new LinkedList();
            hijo = this.consultaSoloHijo2(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra);

            Boolean conteosolohijo = hijo.isEmpty(); // VALIDA SI LA LISTA ESTA VACIA
            if (!conteosolohijo) {

                ConexionSiebel conSiebel = new ConexionSiebel();
                SiebelDataBean m_dataBean = new SiebelDataBean();
                conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

                for (ConjuntoReglasValidacionSoloHijo2 sAdminC : hijo) {  // SI LA LISTA NO ESTA VACIA, COMIENZA EJECUCION
                    if ("SI".equals(Conectar)) {
                        conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                        Conectar = "NO";
                    }
                    try {
                        SiebelBusObject BO = m_dataBean.getBusObject("FINS Validation");
                        SiebelBusComp BC = BO.getBusComp("FINS Validation Rule Set");
                        SiebelBusComp BCCHILD = BO.getBusComp("FINS Validation Rule Set Arguments");
                        SiebelBusComp oBCPick = new SiebelBusComp();

                        BC.activateField("Name");
                        BC.clearToQuery();
                        BC.setSearchExpr("[Name] ='" + sAdminC.getNombreconjuntoregla()+ "' ");  // BUSCA PADRE PARA OBTENER ROW_ID DEL AMBIENTE DESTINO
                        BC.executeQuery(true);
                        boolean reg = BC.firstRecord();
                        if (reg) {
                            String IdPadre = BC.getFieldValue("Id");  // OBTIENE ROW_ID DEL AMBIENTE DESTINO

                            // SI SE ENCONTRO ROW_ID DE PADRE, COMIENZA BUSQUEDA DE HIJOS 1 - CONDICIONES
                            BCCHILD.activateField("Name");
                            BCCHILD.activateField("Default Value");
                            BCCHILD.activateField("Comments");
                            BCCHILD.activateField("Rule Set Id"); // Id del Padre
                            BCCHILD.clearToQuery();
                            BCCHILD.setSearchExpr("[Name]='" + sAdminC.getNombreArgumento() + "' AND [Rule Set Id]='" + IdPadre + "'");
                            BCCHILD.executeQuery(true);
                            boolean regchild = BCCHILD.firstRecord();
                            if (regchild) {
                                if (sAdminC.getValorPredeterminado() != null) {
                                    if (!BCCHILD.getFieldValue("Default Value").equals(sAdminC.getValorPredeterminado())) {
                                        BCCHILD.setFieldValue("Default Value", sAdminC.getValorPredeterminado());
                                    }
                                }

                                if (sAdminC.getComentarios() != null) {
                                    if (!BCCHILD.getFieldValue("Comments").equals(sAdminC.getComentarios())) {
                                        BCCHILD.setFieldValue("Comments", sAdminC.getComentarios());
                                    }
                                }

                                BCCHILD.writeRecord();

                                System.out.println("Se valida existencia y/o actualizacion de Argumento:  " + sAdminC.getNombreArgumento());
                                String MsgSalida = "Se valida existencia y/o actualizacion de Argumento.";

                                this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(MsgSalida);
                                
                                BCCHILD.release();
                                BC.release();
                                BO.release();

                            } else {
                                BCCHILD.newRecord(0);

                                if (sAdminC.getNombreArgumento() != null) {
                                    BCCHILD.setFieldValue("Name", sAdminC.getNombreArgumento());
                                }

                                if (sAdminC.getValorPredeterminado() != null) {
                                    BCCHILD.setFieldValue("Default Value", sAdminC.getValorPredeterminado());
                                }

                                if (sAdminC.getComentarios() != null) {
                                    BCCHILD.setFieldValue("Comments", sAdminC.getComentarios());
                                }

                                BCCHILD.setFieldValue("Rule Set Id", IdPadre);

                                BCCHILD.writeRecord();

                                System.out.println("Se creo Registro de Argumento:  " + sAdminC.getNombreArgumento());
                                String mensaje = ("Se creo Registro de Argumento:  " + sAdminC.getNombreArgumento());

                                String MsgSalida = "Creado Registro de Argumento";
                                this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(mensaje);
                                
                                BCCHILD.release();
                                BC.release();
                                BO.release();
                            }

                        } else {
                            System.out.println("No se encontro registro de Conjunto de Reglas de Validacion: " + sAdminC.getNombreconjuntoregla() + " , en el ambiente a insertar, por esta razon no puede validarse si existen REGLAS a modificar o crear. ");
                            String MsgSalida = "No se encontro registro de Conjunto de Reglas de Validacion en el ambiente a insertar";
                            String FlagCarga = "E";
                            String MsgError = "No se encontro registro de Conjunto de Reglas de Validacion con nombre: " + sAdminC.getNombreconjuntoregla() + " , en el ambiente a insertar, por esta razon no puede validarse si existen REGLAS a modificar o crear. ";

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgError);
                            this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteExtra,ambienteInser);

                            BCCHILD.release();
                            BC.release();
                            BO.release();
                        }

                    } catch (SiebelException e) {
                        String error = e.getErrorMessage();
                        System.out.println("Error en creacion de Hijos - ARGUMENTOS:  " + sAdminC.getNombreArgumento() + "     , con el mensaje:   " + error.replace("'", " "));
                        String MsgSalida = "Error al Crear Hijos - ARGUMENTOS";
                        String FlagCarga = "E";
                        String MsgError = "Error en creacion de Hijos - ARGUMENTOS:  " + sAdminC.getNombreArgumento() + "     , con el mensaje:   " + error.replace("'", " ");

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgError);

                        this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteExtra,ambienteInser);

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
            System.out.println("Error en  validacion Solo Hijo - Reglas, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en  validacion Solo Hijo - Reglas, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);

        }
        
    }

    @Override
    public List<ConjuntoReglasValidacionPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.NAME \"Nombre\", A.RL_SET_CALL_NAME \"Grupo\", A.BUSCOMP_NAME \"Business component\", A.REV_NUM \"Versión\", A.STATUS_CD \"Estado\", A.BUSOBJ_NAME \"Business Object\", A.VIEW_NAME \"Ver\",\n"
                + "A.COND_EXPR \"Expresión condicional\", A.EFF_START_DT \"Fecha de inicio\", A.EFF_END_DT \"Fecha final\", A.AGGREGATE_ERR_FLG \"Agregación de errores\", A.DESC_TEXT \"Descripcion\"\n"
                + "FROM SIEBEL.S_VALDN_RL_SET A, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<ConjuntoReglasValidacionPadre> conjuntoReglasValidacion = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Padre a procesar.");

            while (rs.next()) {
                ConjuntoReglasValidacionPadre lista = new ConjuntoReglasValidacionPadre();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setGrupo(rs.getString("Grupo"));
                lista.setBusinessComponent(rs.getString("Business component"));
                lista.setVersion(rs.getString("Versión"));
                lista.setEstado(rs.getString("Estado"));
                lista.setBusinessObject(rs.getString("Business Object"));
                lista.setVer(rs.getString("Ver"));
                lista.setExpresionCondicional(rs.getString("Expresión condicional"));
                lista.setFechaInicio(rs.getDate("Fecha de inicio"));
                lista.setFechaFinal(rs.getDate("Fecha final"));
                lista.setAgregacionErrores(rs.getString("Agregación de errores"));
                lista.setDescripcion(rs.getString("Descripcion"));

                conjuntoReglasValidacion.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre"), rs.getString("Grupo"), "SE OBTIENEN DATOS", "CONJUNTO DE REGLAS DE VALIDACION",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = conjuntoReglasValidacion.size();
            Boolean conteo = conjuntoReglasValidacion.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Padres de Conjunto de Reglas de Validacion o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Padres de Conjunto de Reglas de Validacion o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Padres de Conjunto de Reglas de Validacion, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Padres de Conjunto de Reglas de Validacion, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return conjuntoReglasValidacion;
    }

    @Override
    public List<ConjuntoReglasValidacionHijo1> consultaHijo1(String IdPadre, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.RULE_SET_ID \"Id Padre\", B.SEQ_NUM \"No de Secuencia\", B.NAME \"Nombre\", B.RULE_EXPR \"Expresión\", B.BUSCOMP_NAME \"Business Component\", \n"
                + "B.APPLY_TO_CD \"Aplicar a\", B.EFF_START_DT \"Fecha de inicio\", B.EFF_END_DT \"Fecha Final\", B.DESC_TEXT \"Descripción\", B.RETURN_CD \"Código de retorno\"\n"
                + "FROM  SIEBEL.S_VALDN_RULE B, SIEBEL.S_USER H\n"
                + "WHERE B.LAST_UPD_BY = H.ROW_ID"
                + "AND B.RULE_SET_ID = '" + IdPadre + "'  AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY B.CREATED ASC";
        List<ConjuntoReglasValidacionHijo1> conjuntoReglasValidacion = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar- Reglas");

            while (rs.next()) {
                ConjuntoReglasValidacionHijo1 lista = new ConjuntoReglasValidacionHijo1();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setNumSecuencia(rs.getString("No de Secuencia"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setExprecion(rs.getString("Expresión"));
                lista.setBusinessComponent(rs.getString("Business Component"));
                lista.setAplicar(rs.getString("Aplicar a"));
                lista.setFechaInicio(rs.getDate("Fecha de inicio"));
                lista.setFechaFinal(rs.getDate("Fecha Final"));
                lista.setDescripcion(rs.getString("Descripción"));
                lista.setCodigoRetorno(rs.getString("Código de retorno"));

                conjuntoReglasValidacion.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("No de Secuencia"), rs.getString("Nombre"), "SE OBTIENEN DATOS HIJO - REGLAS", "CONJUNTO DE REGLAS DE VALIDACION - REGLAS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = conjuntoReglasValidacion.size();
            Boolean conteo = conjuntoReglasValidacion.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Reglas o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Reglas o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Reglas, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Reglas, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo, con el error:  " + ex);
            throw ex;
        }
        return conjuntoReglasValidacion;
    }
    
    
    @Override
    public List<ConjuntoReglasValidacionSoloHijo1> consultaSoloHijo1(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.RULE_SET_ID \"Id Padre\", B.SEQ_NUM \"No de Secuencia\", B.NAME \"Nombre\", B.RULE_EXPR \"Expresión\", B.BUSCOMP_NAME \"Business Component\",\n"
                + "B.APPLY_TO_CD \"Aplicar a\", B.EFF_START_DT \"Fecha de inicio\", B.EFF_END_DT \"Fecha Final\", B.DESC_TEXT \"Descripción\", B.RETURN_CD \"Código de retorno\", A.NAME \"Nombre ConjuntoRegla\"\n"
                + "FROM  SIEBEL.S_VALDN_RULE B, SIEBEL.S_VALDN_RL_SET A,SIEBEL.S_USER H\n"
                + "WHERE B.RULE_SET_ID = A.ROW_ID AND B.LAST_UPD_BY = H.ROW_ID\n"
                + "AND B.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND  H.LOGIN = '" + usuario + "' AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY B.LAST_UPD ASC";
        List<ConjuntoReglasValidacionSoloHijo1> conjuntoReglasValidacion = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Solo Hijos a procesar- Reglas");

            while (rs.next()) {
                ConjuntoReglasValidacionSoloHijo1 lista = new ConjuntoReglasValidacionSoloHijo1();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setNumSecuencia(rs.getString("No de Secuencia"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setExprecion(rs.getString("Expresión"));
                lista.setBusinessComponent(rs.getString("Business Component"));
                lista.setAplicar(rs.getString("Aplicar a"));
                lista.setFechaInicio(rs.getDate("Fecha de inicio"));
                lista.setFechaFinal(rs.getDate("Fecha Final"));
                lista.setDescripcion(rs.getString("Descripción"));
                lista.setCodigoRetorno(rs.getString("Código de retorno"));
                lista.setNombreconjuntoregla(rs.getString("Nombre ConjuntoRegla"));
                conjuntoReglasValidacion.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("No de Secuencia"), rs.getString("Nombre"), "SE OBTIENEN DATOS SOLO HIJO - REGLAS", "CONJUNTO DE REGLAS DE VALIDACION SOLO HIJO - REGLAS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = conjuntoReglasValidacion.size();
            Boolean conteo = conjuntoReglasValidacion.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Solo Hijos - Reglas o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Solo Hijos Reglas o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Solo Hijos - Reglas, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Solo Hijos Reglas, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaSoloHijo1, con el error:  " + ex);
            throw ex;
        }
        return conjuntoReglasValidacion;
    }
    
    

    @Override
    public List<ConjuntoReglasValidacionHijo2> consultaHijo2(String IdPadre, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT C.ROW_ID, C.RULE_SET_ID \"Id Padre\",C.NAME \"Nombre de argumento\", C.VALUE \"Valor predeterminado\", C.DESC_TEXT \"Comentarios\"\n"
                + "FROM SIEBEL.S_VALDN_SET_ARG C, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = C.LAST_UPD_BY"
                + "AND C.RULE_SET_ID = '" + IdPadre + "'\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY C.CREATED ASC";
        List<ConjuntoReglasValidacionHijo2> conjuntoReglasValidacion = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar- Argumentos");

            while (rs.next()) {
                ConjuntoReglasValidacionHijo2 lista = new ConjuntoReglasValidacionHijo2();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setNombreArgumento(rs.getString("Nombre de argumento"));
                lista.setValorPredeterminado(rs.getString("Valor predeterminado"));
                lista.setComentarios(rs.getString("Comentarios"));

                conjuntoReglasValidacion.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre de argumento"), rs.getString("Valor predeterminado"), "SE OBTIENEN DATOS HIJO - ARGUMENTOS", "CONJUNTO DE REGLAS DE VALIDACION - ARGUMENTOS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = conjuntoReglasValidacion.size();
            Boolean conteo = conjuntoReglasValidacion.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Argumentos o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Argumentos o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Argumentos, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Argumentos, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo2, con el error:  " + ex);
            throw ex;
        }
        return conjuntoReglasValidacion;
    }
    
    
    @Override
    public List<ConjuntoReglasValidacionSoloHijo2> consultaSoloHijo2(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT C.ROW_ID, C.RULE_SET_ID \"Id Padre\",C.NAME \"Nombre de argumento\", C.VALUE \"Valor predeterminado\", C.DESC_TEXT \"Comentarios\", A.NAME \"Nombre ConjuntoRegla\"\n"
                + "FROM SIEBEL.S_VALDN_SET_ARG C,  SIEBEL.S_VALDN_RL_SET A, SIEBEL.S_USER H\n"
                + "WHERE C.RULE_SET_ID= A.ROW_ID AND C.LAST_UPD_BY = H.ROW_ID\n"
                + "AND C.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND  H.LOGIN = '" + usuario + "' AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA E WHERE E.ROW_ID = C.ROW_ID AND E.USUARIO ='" + usuario + "' AND E.VERSION ='" + version + "' AND E.EXTRAER ='" + ambienteExtra + "' AND E.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY C.LAST_UPD ASC";
        List<ConjuntoReglasValidacionSoloHijo2> conjuntoReglasValidacion = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Solo Hijos a procesar- Argumentos");

            while (rs.next()) {
                ConjuntoReglasValidacionSoloHijo2 lista = new ConjuntoReglasValidacionSoloHijo2();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setNombreArgumento(rs.getString("Nombre de argumento"));
                lista.setValorPredeterminado(rs.getString("Valor predeterminado"));
                lista.setComentarios(rs.getString("Comentarios"));
                lista.setNombreconjuntoregla(rs.getString("Nombre ConjuntoRegla"));
                conjuntoReglasValidacion.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre de argumento"), rs.getString("Valor predeterminado"), "SE OBTIENEN DATOS SOLO HIJO - ARGUMENTOS", "CONJUNTO DE REGLAS DE VALIDACION SOLO HIJO - ARGUMENTOS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = conjuntoReglasValidacion.size();
            Boolean conteo = conjuntoReglasValidacion.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Solo Hijos - Argumentos o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Solo Hijos Argumentos o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Solo Hijos - Argumentos, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Solo Hijos Argumentos, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaSoloHijo2, con el error:  " + ex);
            throw ex;
        }
        return conjuntoReglasValidacion;
    }
    
    

    @Override
    public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick, String RowID, String IdPadre, String usuario,String version,String ambienteInser,String ambienteExtra) throws SiebelException {
        try {
            List<ConjuntoReglasValidacionHijo1> hijo = new LinkedList();
            hijo = this.consultaHijo1(IdPadre,usuario,version,ambienteInser,ambienteExtra);
            for (ConjuntoReglasValidacionHijo1 sAdminC : hijo) {
                try {
                    BC.activateField("Sequence Num");
                    BC.activateField("Name");
                    BC.activateField("Expression");
                    BC.activateField("Business Component");
                    BC.activateField("Apply To Type");
                    BC.activateField("Start Date");
                    BC.activateField("End Date");
                    BC.activateField("Description");
                    BC.activateField("Return Code");
                    BC.activateField("Rule Set Id"); // Id de Padre
                    BC.clearToQuery();
                    BC.setSearchExpr("[Name]='" + sAdminC.getNombre() + "' ");
                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {
                        if (sAdminC.getNumSecuencia() != null) {
                            if (!BC.getFieldValue("Sequence Num").equals(sAdminC.getNumSecuencia())) {
                                BC.setFieldValue("Sequence Num", sAdminC.getNumSecuencia());
                            }
                        }

                        if (sAdminC.getExprecion() != null) {
                            if (!BC.getFieldValue("Expression").equals(sAdminC.getExprecion())) {
                                BC.setFieldValue("Expression", sAdminC.getExprecion());
                            }
                        }

                        if (sAdminC.getBusinessComponent() != null) {
                            if (!BC.getFieldValue("Business Component").equals(sAdminC.getBusinessComponent())) {
                                oBCPick = BC.getPicklistBusComp("Business Component");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchSpec("Name", sAdminC.getBusinessComponent());
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getAplicar() != null) {
                            if (!BC.getFieldValue("Apply To Type").equals(sAdminC.getAplicar())) {
                                oBCPick = BC.getPicklistBusComp("Apply To Type");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchSpec("Value", sAdminC.getAplicar());
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getFechaInicio() != null) {
                            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                            String dateString = format.format(sAdminC.getFechaInicio());
                            if (!BC.getFieldValue("Activation Date/Time").equals(dateString)) {
                                BC.setFieldValue("Start Date", dateString);
                            }
                        }

                        if (sAdminC.getFechaFinal() != null) {
                            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                            String dateString = format.format(sAdminC.getFechaFinal());
                            if (!BC.getFieldValue("Activation Date/Time").equals(dateString)) {
                                BC.setFieldValue("End Date", dateString);
                            }
                        }

                        if (sAdminC.getDescripcion() != null) {
                            if (!BC.getFieldValue("Activation Date/Time").equals(sAdminC.getDescripcion())) {
                                BC.setFieldValue("Description", sAdminC.getDescripcion());
                            }
                        }

                        if (sAdminC.getAplicar() != null) {
                            if (!BC.getFieldValue("Return Code").equals(sAdminC.getAplicar())) {
                                oBCPick = BC.getPicklistBusComp("Return Code");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchSpec("Message Code", sAdminC.getCodigoRetorno());
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }

                            BC.writeRecord();

                            System.out.println("Se valida existencia y/o actualizacion de Reglas :  " + sAdminC.getNombre());
                            String MsgSalida = "Se valida existencia y/o actualizacion de Reglas";

                            this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgSalida);

                        }
                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getNumSecuencia() != null) {
                            BC.setFieldValue("Sequence Num", sAdminC.getNumSecuencia());
                        }

                        if (sAdminC.getNombre() != null) {
                            BC.setFieldValue("Name", sAdminC.getNombre());
                        }

                        if (sAdminC.getExprecion() != null) {
                            BC.setFieldValue("Expression", sAdminC.getExprecion());
                        }

                        if (sAdminC.getBusinessComponent() != null) {
                            oBCPick = BC.getPicklistBusComp("Business Component");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchSpec("Name", sAdminC.getBusinessComponent());
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getAplicar() != null) {
                            oBCPick = BC.getPicklistBusComp("Apply To Type");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchSpec("Value", sAdminC.getAplicar());
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getFechaInicio() != null) {
                            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                            String dateString = format.format(sAdminC.getFechaInicio());
                            BC.setFieldValue("Start Date", dateString);
                        }

                        if (sAdminC.getFechaFinal() != null) {
                            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                            String dateString = format.format(sAdminC.getFechaFinal());
                            BC.setFieldValue("End Date", dateString);
                        }

                        if (sAdminC.getDescripcion() != null) {
                            BC.setFieldValue("Description", sAdminC.getDescripcion());
                        }

                        if (sAdminC.getAplicar() != null) {
                            oBCPick = BC.getPicklistBusComp("Return Code");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchSpec("Message Code", sAdminC.getCodigoRetorno());
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        BC.setFieldValue("Rule Set Id", RowID);
                        BC.writeRecord();
                        System.out.println("Se creo Regla:  " + sAdminC.getNombre());
                        String mensaje = ("Se creo Regla:  " + sAdminC.getNombre());

                        String MsgSalida = "Creado Regla";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);
                    }
                } catch (SiebelException e) {
                    
                    String error = e.getErrorMessage();
                    System.out.println("Error en creacion de Regla:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Hijo-Conjunto de Reglas de Validacion";
                    String FlagCarga = "E";
                    String MsgError = "Error en creacion de Regla:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " ");

                    this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgError);
      }
            }
        } catch (SiebelException e) {
           String error = e.getErrorMessage();
                System.out.println("Error en cargaBC, con el error:  " + error.replace("'", " "));
                String MsgError = "Error en cargaBC, con el error:  " +  error.replace("'", " ");
                Reportes rep = new Reportes();
                rep.agregarTextoAlfinal(MsgError);
        } catch (Exception ex) {
            Logger.getLogger(DAOConjuntoReglasValidacionImpl.class.getName()).log(Level.SEVERE, null, ex);
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

    private void cargaBC2(SiebelBusComp BC, SiebelBusComp oBCPick, String RowID, String IdPadre, String usuario,String version,String ambienteInser,String ambienteExtra) throws SiebelException {
        try {
            List<ConjuntoReglasValidacionHijo2> hijo = new LinkedList();
            hijo = this.consultaHijo2(IdPadre,usuario,version,ambienteInser,ambienteExtra);
            for (ConjuntoReglasValidacionHijo2 sAdminC : hijo) {
                try {
                    BC.activateField("Name");
                    BC.activateField("Default Value");
                    BC.activateField("Comments");
                    BC.activateField("Rule Set Id"); // Id del Padre
                    BC.clearToQuery();
                    BC.setSearchSpec("Name", sAdminC.getNombreArgumento());
                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {
                        if (sAdminC.getValorPredeterminado() != null) {
                            if (!BC.getFieldValue("Default Value").equals(sAdminC.getValorPredeterminado())) {
                                BC.setFieldValue("Default Value", sAdminC.getValorPredeterminado());
                            }
                        }

                        if (sAdminC.getComentarios() != null) {
                            if (!BC.getFieldValue("Comments").equals(sAdminC.getComentarios())) {
                                BC.setFieldValue("Comments", sAdminC.getComentarios());
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Argumento:  " + sAdminC.getNombreArgumento());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Argumento.";

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getNombreArgumento() != null) {
                            BC.setFieldValue("Name", sAdminC.getNombreArgumento());
                        }

                        if (sAdminC.getValorPredeterminado() != null) {
                            BC.setFieldValue("Default Value", sAdminC.getValorPredeterminado());
                        }

                        if (sAdminC.getComentarios() != null) {
                            BC.setFieldValue("Comments", sAdminC.getComentarios());
                        }

                        BC.setFieldValue("Rule Set Id", RowID);

                        BC.writeRecord();

                        System.out.println("Se creo Argumento:  " + sAdminC.getNombreArgumento());
                        String mensaje = ("Se creo Argumento:  " + sAdminC.getNombreArgumento());

                        String MsgSalida = "Creado Argumento";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);
                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en creacion de Argumento:  " + sAdminC.getNombreArgumento() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Hijo2-Conjunto de Reglas de Validacion";
                    String FlagCarga = "E";
                    String MsgError = "Error en creacion de Argumento:  " + sAdminC.getNombreArgumento() + "     , con el mensaje:   " + error.replace("'", " ");

                    this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgError);
                   
                }

            }
        } catch (SiebelException e) {
            String error = e.getErrorMessage();
                System.out.println("Error en cargaBC2, con el error:  " + error.replace("'", " "));
                String MsgError = "Error en cargaBC2, con el error:  " +  error.replace("'", " ");
                Reportes rep = new Reportes();
                rep.agregarTextoAlfinal(MsgError);
						
        } catch (Exception ex) {
            Logger.getLogger(DAOConjuntoReglasValidacionImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void CargaBitacoraEntrada(String Row_Id, String Val1, String Val2, String Seguimiento, String Objeto, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

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
