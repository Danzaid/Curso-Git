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
import interfaces.DAOMatricesElegibilidadComp;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import objetos.MatricesElegibilidadCompHijo1;
import objetos.MatricesElegibilidadCompHijo2;
import objetos.MatricesElegibilidadCompPadre;
import objetos.MatricesElegibilidadCompSoloHijo1;
import objetos.MatricesElegibilidadCompSoloHijo2;

/**
 *
 * @author hector.pineda
 */
public class DAOMatricesElegibilidadCompImpl extends ConexionDB implements DAOMatricesElegibilidadComp {

    @Override
    public void inserta(List<MatricesElegibilidadCompPadre> matricesElegibilidadComp, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);

        Boolean conteopadre = matricesElegibilidadComp.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

            for (MatricesElegibilidadCompPadre sAdminC : matricesElegibilidadComp) {
                if ("SI".equals(Conectar)) {
                    conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                    Conectar = "NO";
                }

                try {

                    String MsgError = "";

                    SiebelBusObject BO = m_dataBean.getBusObject("Adjustment Group");
                    SiebelBusComp BC = BO.getBusComp("Adjustment Group");
                    SiebelBusComp BCCHILD = BO.getBusComp("Product Eligibility BusComp");
                    SiebelBusComp BCCHILD2 = BO.getBusComp("Product Compatibility");
                    SiebelBusComp oBCPick = new SiebelBusComp();

                    BC.activateField("Name");
                    BC.activateField("Adjustment Group Type E&C");
                    BC.clearToQuery();
                    BC.setSearchExpr("[Name] ='" + sAdminC.getNombre() + "' ");
                    BC.executeQuery(true);
                    boolean reg = BC.firstRecord();
                    if (reg) {
                        if (sAdminC.getTipoMatriz() != null) {
                            if (!BC.getFieldValue("Adjustment Group Type E&C").equals(sAdminC.getTipoMatriz())) {
                                oBCPick = BC.getPicklistBusComp("Adjustment Group Type E&C");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Value]='" + sAdminC.getTipoMatriz() + "'");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Matrices de Elegibilidad y Compatibilidad:  " + sAdminC.getNombre() + " : " + sAdminC.getTipoMatriz());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Matrices de Elegibilidad y Compatibilidad";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteExtra,ambienteInser);

//                        String RowId1 = BC.getFieldValue("Id");

//                        if ("Reglas de elegibilidad".equals(sAdminC.getTipoMatriz())) {
//                            this.cargaBC(BCCHILD, oBCPick, RowId1, sAdminC.getRowId(),usuario,version,ambienteExtra,ambienteInser);  // REGLAS DE ELEGIBILIDAD
//
//                            BCCHILD2.release();
//                            BCCHILD.release();
//                            BC.release();
//                            BO.release();
//                        }
//
//                        if ("Reglas de compatibilidad".equals(sAdminC.getTipoMatriz())) {
//                            this.cargaBC2(BCCHILD2, oBCPick, RowId1, sAdminC.getRowId(),usuario,version,ambienteExtra,ambienteInser);  // REGLAS DE COMPATIBILIDAD 
//
//                            BCCHILD2.release();
//                            BCCHILD.release();
//                            BC.release();
//                            BO.release();
//                        }
                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getNombre() != null) {
                            BC.setFieldValue("Name", sAdminC.getNombre());
                        }

                        if (sAdminC.getTipoMatriz() != null) {
                            oBCPick = BC.getPicklistBusComp("Adjustment Group Type E&C");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value]='" + sAdminC.getTipoMatriz() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se crea registro de Matrices de Elegibilidad y Compatibilidad: " + sAdminC.getNombre() + " : " + sAdminC.getTipoMatriz());
                        String mensaje = ("Se crea registro de Matrices de Elegibilidad y Compatibilidad: " + sAdminC.getNombre() + " : " + sAdminC.getTipoMatriz());

                        String MsgSalida = "Creado Matrices de Elegibilidad y Compatibilidad";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

//                        String RowID = BC.getFieldValue("Id");
//
//                        if ("Reglas de elegibilidad".equals(sAdminC.getTipoMatriz())) {
//                            this.cargaBC(BCCHILD, oBCPick, RowID, sAdminC.getRowId(),usuario,version,ambienteInser,ambienteExtra);  // REGLAS DE ELEGIBILIDAD
//
//                            BCCHILD2.release();
//                            BCCHILD.release();
//                            BC.release();
//                            BO.release();
//
//                        }
//
//                        if ("Reglas de compatibilidad".equals(sAdminC.getTipoMatriz())) {
//                            this.cargaBC2(BCCHILD2, oBCPick, RowID, sAdminC.getRowId(),usuario,version,ambienteInser,ambienteExtra); // REGLAS DE COMPATIBILIDAD
//
//                            BCCHILD2.release();
//                            BCCHILD.release();
//                            BC.release();
//                            BO.release();
//                        }

                    }
                } catch (SiebelException e) {
                    System.out.println("Error en creacion Matrices de Elegibilidad y Compatibilidad:  " + sAdminC.getNombre() + " : " + sAdminC.getTipoMatriz() + "     , con el mensaje:   " + e);
                    String MsgSalida = "Error al Crear Matrices de Elegibilidad y Compatibilidad";
                    String FlagCarga = "E";
                    String MsgError = "Error en creacion Matrices de Elegibilidad y Compatibilidad:  " + sAdminC.getNombre() + " : " + sAdminC.getTipoMatriz() + "     , con el mensaje:   " + e;

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

        try {                                                                       // BUSCA REGISTROS NUEVOS O ACTUALIZADOS, DESDE EL HIJO 1 - ELIGIBILIDAD
            List<MatricesElegibilidadCompSoloHijo1> hijo = new LinkedList();
            hijo = this.consultaSoloHijo1(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra);

            Boolean conteosolohijo = hijo.isEmpty(); // VALIDA SI LA LISTA ESTA VACIA
            if (!conteosolohijo) {

                ConexionSiebel conSiebel = new ConexionSiebel();
                SiebelDataBean m_dataBean = new SiebelDataBean();
                conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

                for (MatricesElegibilidadCompSoloHijo1 sAdminC : hijo) {  // SI LA LISTA NO ESTA VACIA, COMIENZA EJECUCION
                    if ("SI".equals(Conectar)) {
                        conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                        Conectar = "NO";
                    }
                    try {
                        SiebelBusObject BO = m_dataBean.getBusObject("Adjustment Group");
                        SiebelBusComp BC = BO.getBusComp("Adjustment Group");
                        SiebelBusComp BCCHILD = BO.getBusComp("Product Eligibility BusComp");
                        SiebelBusComp oBCPick = new SiebelBusComp();

                        BC.activateField("Name");
                        BC.clearToQuery();
                        BC.setSearchExpr("[Name] ='" + sAdminC.getNombrematriz() + "' ");  // BUSCA PADRE PARA OBTENER ROW_ID DEL AMBIENTE DESTINO
                        BC.executeQuery(true);
                        boolean reg = BC.firstRecord();
                        if (reg) {
                            String IdPadre = BC.getFieldValue("Id");  // OBTIENE ROW_ID DEL AMBIENTE DESTINO

                            // SI SE ENCONTRO ROW_ID DE PADRE, COMIENZA BUSQUEDA DE HIJOS 1 - CONDICIONES
                            BCCHILD.activateField("Product");
                            BCCHILD.activateField("Type");
                            BCCHILD.activateField("Action Code");
                            BCCHILD.activateField("TT RPT");
                            BCCHILD.activateField("State");
                            BCCHILD.activateField("Country");
                            BCCHILD.activateField("Adjustment Group Id"); // Id de Padre
                            BCCHILD.clearToQuery();
                            
                            if(sAdminC.getCiudad() != null){
                                BCCHILD.setSearchExpr("[Product] ='" + sAdminC.getProducto() + "' AND [Type] = '" + sAdminC.getTipoRegla() + "'  AND [TT RPT] = '" + sAdminC.getCiudad() + "'  AND [Adjustment Group Id] = '" + IdPadre + "'");
                            }else{
                            BCCHILD.setSearchExpr("[Product] ='" + sAdminC.getProducto() + "' AND [Type] = '" + sAdminC.getTipoRegla() + "' AND [Adjustment Group Id] = '" + IdPadre + "'");
                            }
                            
                            BCCHILD.executeQuery(true);
                            boolean regchild = BCCHILD.firstRecord();
                            if (regchild) {

//                                if (sAdminC.getTipoRegla() != null) {
//                                    if (!BCCHILD.getFieldValue("Type").equals(sAdminC.getTipoRegla())) {
//                                        oBCPick = BCCHILD.getPicklistBusComp("Type");
//                                        oBCPick.clearToQuery();
//                                        oBCPick.setSearchExpr("[Value]='" + sAdminC.getTipoRegla() + "'");
//                                        oBCPick.executeQuery(true);
//                                        if (oBCPick.firstRecord()) {
//                                            oBCPick.pick();
//                                        }
//                                        oBCPick.release();
//                                    }
//                                }

                                if (sAdminC.getCodigoAccionAsset() != null) {
                                    if (!BCCHILD.getFieldValue("Action Code").equals(sAdminC.getCodigoAccionAsset())) {
                                        BCCHILD.setFieldValue("Action Code", sAdminC.getCodigoAccionAsset());
                                    }
                                }

                                if (sAdminC.getRegion() != null) {
                                    if (!BCCHILD.getFieldValue("State").equals(sAdminC.getRegion())) {
                                        oBCPick = BCCHILD.getPicklistBusComp("State");
                                        oBCPick.clearToQuery();
                                        oBCPick.setSearchExpr("[Value]='" + sAdminC.getRegion() + "'");
                                        oBCPick.executeQuery(true);
                                        if (oBCPick.firstRecord()) {
                                            oBCPick.pick();
                                        }
                                        oBCPick.release();
                                    }
                                }

                                if (sAdminC.getPais() != null) {
                                    if (!BCCHILD.getFieldValue("Country").equals(sAdminC.getPais())) {
                                        oBCPick = BCCHILD.getPicklistBusComp("Country");
                                        oBCPick.clearToQuery();
                                        oBCPick.setSearchExpr("[Value]='" + sAdminC.getPais() + "'");
                                        oBCPick.executeQuery(true);
                                        if (oBCPick.firstRecord()) {
                                            oBCPick.pick();
                                        }
                                        oBCPick.release();
                                    }
                                }
                                
                                if (sAdminC.getCiudad() != null) {
                                    if (!BCCHILD.getFieldValue("TT RPT").equals(sAdminC.getCiudad())) {
                                        BCCHILD.setFieldValue("TT RPT", sAdminC.getCiudad());
                                    }
                                }

                                BCCHILD.writeRecord();

                                System.out.println("Se valida existencia y/o actualizacion de Hijo - Elegibilidad:  " + sAdminC.getProducto() + " : " + sAdminC.getCiudad());
                                String MsgSalida = "Se valida existencia y/o actualizacion de Hijo - Elegibilidad";
                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(MsgSalida);

                                this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                BCCHILD.release();
                                BC.release();
                                BO.release();

                            } else {
                                BCCHILD.newRecord(0);

                                if (sAdminC.getProducto() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("Product");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Name]='" + sAdminC.getProducto() + "'");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getTipoRegla() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("Type");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Value]='" + sAdminC.getTipoRegla() + "'");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getCodigoAccionAsset() != null) {
                                    BCCHILD.setFieldValue("Action Code", sAdminC.getCodigoAccionAsset());
                                }

                                if (sAdminC.getCiudad() != null) {
                                    BCCHILD.setFieldValue("TT RPT", sAdminC.getCiudad());
                                }

                                if (sAdminC.getRegion() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("State");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Value]='" + sAdminC.getRegion() + "'");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getPais() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("Country");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Value]='" + sAdminC.getPais() + "'");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                BCCHILD.setFieldValue("Adjustment Group Id", IdPadre);

                                BCCHILD.writeRecord();

                                System.out.println("Se creo registro de Hijo - Elegibilidad:  " + sAdminC.getProducto() + " : " + sAdminC.getCiudad());
                                String mensaje = ("Se creo registro de Hijo - Elegibilidad:  " + sAdminC.getProducto() + " : " + sAdminC.getCiudad());

                                String MsgSalida = "Creado registro de Hijo - Elegibilidad";
                                this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(mensaje);

                                BCCHILD.release();
                                BC.release();
                                BO.release();
                            }

                        } else {
                            System.out.println("No se encontro registro de Matriz con nombre: " + sAdminC.getNombrematriz() + " , en el ambiente a insertar, por esta razon no puede validarse si existen ELEGIBILIDAD a modificar o crear. ");
                            String MsgSalida = "No se encontro registro de Matriz de Elegibilidad en el ambiente a insertar";
                            String FlagCarga = "E";
                            String MsgError = "No se encontro registro de Matriz con nombre: " + sAdminC.getNombrematriz() + " , en el ambiente a insertar, por esta razon no puede validarse si existen ELEGIBILIDAD a modificar o crear. ";

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgError);
                            this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                            BCCHILD.release();
                            BC.release();
                            BO.release();
                        }

                    } catch (SiebelException e) {
                        String error = e.getErrorMessage();
                        System.out.println("Error en creacion de Hijos - Elegibilidad:  " + sAdminC.getProducto() + " : " + sAdminC.getCiudad() + "     , con el mensaje:   " + error.replace("'", " "));
                        String MsgSalida = "Error al Crear Hijos - Elegibilidad";
                        String FlagCarga = "E";
                        String MsgError = "Error en creacion de Hijos - Elegibilidad:  " + sAdminC.getProducto() + " : " + sAdminC.getCiudad() + "     , con el mensaje:   " + error.replace("'", " ");

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
            System.out.println("Error en  validacion Solo Hijo - Matrices de Elegibilidad, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en  validacion Solo Hijo - Matrices de Elegibilidad, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);

        }
        
        
        
        
        
    try {                                                                       // BUSCA REGISTROS NUEVOS O ACTUALIZADOS, DESDE EL HIJO 2 - COMPATIBILIDAD
            List<MatricesElegibilidadCompSoloHijo2> hijo = new LinkedList();
            hijo = this.consultaSoloHijo2(fechaIni, fechaTer,usuario,version,ambienteInser,ambienteExtra);

            Boolean conteosolohijo = hijo.isEmpty(); // VALIDA SI LA LISTA ESTA VACIA
            if (!conteosolohijo) {

                ConexionSiebel conSiebel = new ConexionSiebel();
                SiebelDataBean m_dataBean = new SiebelDataBean();
                conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

                for (MatricesElegibilidadCompSoloHijo2 sAdminC : hijo) {  // SI LA LISTA NO ESTA VACIA, COMIENZA EJECUCION
                    if ("SI".equals(Conectar)) {
                        conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                        Conectar = "NO";
                    }
                    try {
                        SiebelBusObject BO = m_dataBean.getBusObject("Adjustment Group");
                        SiebelBusComp BC = BO.getBusComp("Adjustment Group");
                        SiebelBusComp BCCHILD = BO.getBusComp("Product Compatibility");
                        SiebelBusComp oBCPick = new SiebelBusComp();

                        BC.activateField("Name");
                        BC.clearToQuery();
                        BC.setSearchExpr("[Name] ='" + sAdminC.getNombrematriz() + "' ");  // BUSCA PADRE PARA OBTENER ROW_ID DEL AMBIENTE DESTINO
                        BC.executeQuery(true);
                        boolean reg = BC.firstRecord();
                        if (reg) {
                            String IdPadre = BC.getFieldValue("Id");  // OBTIENE ROW_ID DEL AMBIENTE DESTINO

                            // SI SE ENCONTRO ROW_ID DE PADRE, COMIENZA BUSQUEDA DE HIJOS 1 - CONDICIONES
                            BCCHILD.activateField("Type");
                            BCCHILD.activateField("Effective Start Date");
                            BCCHILD.activateField("Effective End Date");
                            BCCHILD.activateField("Product"); // Asunto Producto
                            BCCHILD.activateField("Rel Product");  // Objeto Producto
                            BCCHILD.activateField("Adjustment Group Id"); // Id de Padre
                            BCCHILD.clearToQuery();
                            BCCHILD.setSearchExpr("[Product] ='" + sAdminC.getAsuntoProducto() + "' AND [Rel Product] = '" + sAdminC.getObjetoProducto() + "' AND [Adjustment Group Id] ='" + IdPadre + "'");
                            BCCHILD.executeQuery(true);
                            boolean regchild = BCCHILD.firstRecord();
                            if (regchild) {
                                if (sAdminC.getTipo() != null) {
                                    if (!BCCHILD.getFieldValue("Type").equals(sAdminC.getTipo())) {
                                        oBCPick = BCCHILD.getPicklistBusComp("Type");
                                        oBCPick.clearToQuery();
                                        oBCPick.setSearchExpr("[Value]='" + sAdminC.getTipo() + "' ");
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
                                    if (!BCCHILD.getFieldValue("Effective Start Date").equals(dateString)) {
                                        BCCHILD.setFieldValue("Effective Start Date", dateString);
                                    }
                                }

                                if (sAdminC.getFechaFinal() != null) {
                                    SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                                    String dateString = format.format(sAdminC.getFechaFinal());
                                    if (!BCCHILD.getFieldValue("Effective End Date").equals(dateString)) {
                                        BCCHILD.setFieldValue("Effective End Date", dateString);
                                    }
                                }

                                BCCHILD.writeRecord();

                                System.out.println("Se valida existencia y/o actualizacion de Hijo - Compatibilidad:  " + sAdminC.getAsuntoProducto() + " : " + sAdminC.getObjetoProducto());
                                String MsgSalida = "Se valida existencia y/o actualizacion de Hijo - Compatibilidad";
                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(MsgSalida);

                                this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);
                            } else {
                                BCCHILD.newRecord(0);

                                if (sAdminC.getTipo() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("Type");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Value]='" + sAdminC.getTipo() + "' ");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getFechaInicio() != null) {
                                    SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                                    String dateString = format.format(sAdminC.getFechaInicio());
                                    BCCHILD.setFieldValue("Effective Start Date", dateString);
                                }

                                if (sAdminC.getFechaFinal() != null) {
                                    SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                                    String dateString = format.format(sAdminC.getFechaFinal());
                                    BCCHILD.setFieldValue("Effective End Date", dateString);
                                }

                                if (sAdminC.getAsuntoProducto() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("Product");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Name]='" + sAdminC.getAsuntoProducto() + "' ");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getObjetoProducto() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("Rel Product");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Name]='" + sAdminC.getObjetoProducto() + "' ");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                BCCHILD.setFieldValue("Adjustment Group Id", IdPadre);

                                BCCHILD.writeRecord();

                                System.out.println("Se creo registro de Hijo - Compatibilidad:  " + sAdminC.getAsuntoProducto() + " : " + sAdminC.getObjetoProducto());
                                String mensaje = ("Se creo registro de Hijo - Compatibilidad:  " + sAdminC.getAsuntoProducto() + " : " + sAdminC.getObjetoProducto());

                                String MsgSalida = "Creado registro de Hijo - Compatibilidad";
                                this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(mensaje);
                            }

                        } else {
                            System.out.println("No se encontro registro de Matriz con nombre: " + sAdminC.getNombrematriz() + " , en el ambiente a insertar, por esta razon no puede validarse si existen COMPATIBILIDAD a modificar o crear. ");
                            String MsgSalida = "No se encontro registro de Matriz de Elegibilidad en el ambiente a insertar";
                            String FlagCarga = "E";
                            String MsgError = "No se encontro registro de Matriz con nombre: " + sAdminC.getNombrematriz() + " , en el ambiente a insertar, por esta razon no puede validarse si existen COMPATIBILIDAD a modificar o crear. ";

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgError);
                            this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                            BCCHILD.release();
                            BC.release();
                            BO.release();
                        }

                    } catch (SiebelException e) {
                        String error = e.getErrorMessage();
                        System.out.println("Error en creacion de Hijos - Compatibilidad:  " + sAdminC.getAsuntoProducto() + " : " + sAdminC.getObjetoProducto() + "     , con el mensaje:   " + error.replace("'", " "));
                        String MsgSalida = "Error al Crear Hijos - Compatibilidad";
                        String FlagCarga = "E";
                        String MsgError = "Error en creacion de Hijos - Compatibilidad:  " + sAdminC.getAsuntoProducto() + " : " + sAdminC.getObjetoProducto() + "     , con el mensaje:   " + error.replace("'", " ");

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
            System.out.println("Error en  validacion Solo Hijo - Matrices de Compatibilidad, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en  validacion Solo Hijo - Matrices de Compatibilidad, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);

        }

    }

    @Override
    public List<MatricesElegibilidadCompPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID,A.ADJ_GROUP_NAME \"Nombre\",A.TYPE_CD \"Tipo de Matriz\"\n"
                + "FROM SIEBEL.S_ADJ_GROUP A, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<MatricesElegibilidadCompPadre> matricesElegibilidadComp = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Padre a procesar.");

            while (rs.next()) {
                MatricesElegibilidadCompPadre lista = new MatricesElegibilidadCompPadre();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setTipoMatriz(rs.getString("Tipo de Matriz"));
                matricesElegibilidadComp.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre"), rs.getString("Tipo de Matriz"), "SE OBTIENEN DATOS", "MATRICES DE ELEGIBILIDAD Y COMPATIBILIDAD",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = matricesElegibilidadComp.size();
            Boolean conteo = matricesElegibilidadComp.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Padres de Matrices de Elegibilidad y Compatibilidad o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Padres de Matrices de Elegibilidad y Compatibilidad o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Padres de Matrices de Elegibilidad y Compatibilidad, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Padres de Matrices de Elegibilidad y Compatibilidad, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return matricesElegibilidadComp;
    }

    @Override
    public List<MatricesElegibilidadCompHijo1> consultaHijo1(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.ADJ_GROUP_ID \"Id Padre\", C.NAME \"Producto\", B.MTRX_RULE_TYPE_CD \"Tipo de Regla\", B.X_ACTION_CODE \"Codigo Accion Asset\", B.X_TT_RPT \"Ciudad\", \n"
                + "B.STATE \"Región\", B.COUNTRY \"País\"\n"
                + "FROM SIEBEL.S_PRODELIG_MTRX B, SIEBEL.S_PROD_INT C, SIEBEL.S_USER H\n"
                + "WHERE B.PROD_ID = C.ROW_ID AND B.LAST_UPD_BY = H.ROW_ID\n"
                + "AND B.ADJ_GROUP_ID = '" + IdPadre + "' AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY B.LAST_UPD ASC";
        List<MatricesElegibilidadCompHijo1> matricesElegibilidadComp = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar: Matrices de Elegibilidad");

            while (rs.next()) {
                MatricesElegibilidadCompHijo1 lista = new MatricesElegibilidadCompHijo1();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setProducto(rs.getString("Producto"));
                lista.setTipoRegla(rs.getString("Tipo de Regla"));
                lista.setCodigoAccionAsset(rs.getString("Codigo Accion Asset"));
                lista.setCiudad(rs.getString("Ciudad"));
                lista.setRegion(rs.getString("Región"));
                lista.setPais(rs.getString("País"));
                matricesElegibilidadComp.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Producto"), rs.getString("Tipo de Regla"), "SE OBTIENEN DATOS HIJO - ELEGIBILIDAD", "MATRICES DE ELEGIBILIDAD Y COMPATIBILIDAD - HIJOS: ELEGIBILIDAD",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = matricesElegibilidadComp.size();
            Boolean conteo = matricesElegibilidadComp.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Listas de Elegibilidad o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Listas de Elegibilidad o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Listas de Elegibilidad, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Listas de Elegibilidad, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo, con el error:  " + ex);
            throw ex;
        }
        return matricesElegibilidadComp;
    }
    
    
    @Override
    public List<MatricesElegibilidadCompSoloHijo1> consultaSoloHijo1(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.ADJ_GROUP_ID \"Id Padre\", C.NAME \"Producto\", B.MTRX_RULE_TYPE_CD \"Tipo de Regla\", B.X_ACTION_CODE \"Codigo Accion Asset\", B.X_TT_RPT \"Ciudad\", \n"
                + "B.STATE \"Región\", B.COUNTRY \"País\", A.ADJ_GROUP_NAME \"Nombre Matriz\"\n"
                + "FROM SIEBEL.S_PRODELIG_MTRX B, SIEBEL.S_PROD_INT C, SIEBEL.S_ADJ_GROUP A,SIEBEL.S_USER H\n"
                + "WHERE B.PROD_ID = C.ROW_ID AND B.ADJ_GROUP_ID = A.ROW_ID AND B.LAST_UPD_BY = H.ROW_ID\n"
                + "AND B.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND  H.LOGIN = '" + usuario + "' AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY B.LAST_UPD ASC";
        List<MatricesElegibilidadCompSoloHijo1> matricesElegibilidadComp = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar: Matrices de Elegibilidad");

            while (rs.next()) {
                MatricesElegibilidadCompSoloHijo1 lista = new MatricesElegibilidadCompSoloHijo1();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setProducto(rs.getString("Producto"));
                lista.setTipoRegla(rs.getString("Tipo de Regla"));
                lista.setCodigoAccionAsset(rs.getString("Codigo Accion Asset"));
                lista.setCiudad(rs.getString("Ciudad"));
                lista.setRegion(rs.getString("Región"));
                lista.setPais(rs.getString("País"));
                lista.setNombrematriz(rs.getString("Nombre Matriz"));
                matricesElegibilidadComp.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Producto"), rs.getString("Tipo de Regla"), "SE OBTIENEN DATOS HIJO - ELEGIBILIDAD", "MATRICES DE ELEGIBILIDAD Y COMPATIBILIDAD HIJO - ELEGIBILIDAD",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = matricesElegibilidadComp.size();
            Boolean conteo = matricesElegibilidadComp.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Solo Hijos - Listas de Elegibilidad o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Solo Hijos Listas de Elegibilidad o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Solo Hijos - Listas de Elegibilidad, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Solo Hijos Listas de Elegibilidad, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaSoloHijo1, con el error:  " + ex);
            throw ex;
        }
        return matricesElegibilidadComp;
    }
    
    

    @Override
    public List<MatricesElegibilidadCompHijo2> consultaHijo2(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT  B.ROW_ID, B.ADJ_GROUP_ID \"Id Padre\", B.MTRX_RULE_NUM \"Id Regla\", B.COMP_TYPE_CD \"Tipo\", B.EFF_START_DT \"Fecha de Inicio\", B.EFF_END_DT \"Fecha Final\", \n"
                + "C.NAME \"Asunto – Producto\", D.NAME \"Objeto - Producto\"\n"
                + "FROM SIEBEL.S_PRODCOMP_MTRX B, SIEBEL.S_PROD_INT C, SIEBEL.S_PROD_INT D,SIEBEL.S_USER H\n"
                + "WHERE B.PROD_ID = C.ROW_ID AND B.REL_PROD_ID = D.ROW_ID AND B.LAST_UPD_BY = H.ROW_ID\n"
                + "AND B.ADJ_GROUP_ID = '" + IdPadre + "'\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA E WHERE E.ROW_ID = B.ROW_ID AND E.USUARIO ='" + usuario + "' AND E.VERSION ='" + version + "' AND E.EXTRAER ='" + ambienteExtra + "' AND E.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY B.LAST_UPD ASC";
        List<MatricesElegibilidadCompHijo2> matricesElegibilidadComp = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar: Matrices de Compatibilidad");

            while (rs.next()) {
                MatricesElegibilidadCompHijo2 lista = new MatricesElegibilidadCompHijo2();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setIdRegla(rs.getString("Id Regla"));
                lista.setTipo(rs.getString("Tipo"));
                lista.setFechaInicio(rs.getDate("Fecha de Inicio"));
                lista.setFechaFinal(rs.getDate("Fecha Final"));
                lista.setAsuntoProducto(rs.getString("Asunto – Producto"));
                lista.setObjetoProducto(rs.getString("Objeto - Producto"));
                matricesElegibilidadComp.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Id Regla"), rs.getString("Tipo"), "SE OBTIENEN DATOS HIJO - COMPATIBILIDAD", "MATRICES DE ELEGIBILIDAD Y COMPATIBILIDAD HIJO - COMPATIBILIDAD",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = matricesElegibilidadComp.size();
            Boolean conteo = matricesElegibilidadComp.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Listas de Compatibilidad o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Listas de Compatibilidad o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Listas de Compatibilidad, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Listas de Compatibilidad, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo2, con el error:  " + ex);
            throw ex;
        }
        return matricesElegibilidadComp;
    }
    
    @Override
    public List<MatricesElegibilidadCompSoloHijo2> consultaSoloHijo2(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT  B.ROW_ID, B.ADJ_GROUP_ID \"Id Padre\", B.MTRX_RULE_NUM \"Id Regla\", B.COMP_TYPE_CD \"Tipo\", B.EFF_START_DT \"Fecha de Inicio\", B.EFF_END_DT \"Fecha Final\", \n"
                + "C.NAME \"Asunto – Producto\", D.NAME \"Objeto - Producto\", A.ADJ_GROUP_NAME \"Nombre Matriz\"\n"
                + "FROM SIEBEL.S_PRODCOMP_MTRX B, SIEBEL.S_PROD_INT C, SIEBEL.S_PROD_INT D, SIEBEL.S_ADJ_GROUP A, SIEBEL.S_USER H\n"
                + "WHERE B.PROD_ID = C.ROW_ID AND B.REL_PROD_ID = D.ROW_ID AND B.ADJ_GROUP_ID = A.ROW_ID AND B.LAST_UPD_BY = H.ROW_ID\n"
                + "AND B.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND  H.LOGIN = '" + usuario + "' AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA E WHERE E.ROW_ID = B.ROW_ID AND E.USUARIO ='" + usuario + "' AND E.VERSION ='" + version + "' AND E.EXTRAER ='" + ambienteExtra + "' AND E.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY B.LAST_UPD ASC";
        List<MatricesElegibilidadCompSoloHijo2> matricesElegibilidadComp = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar: Matrices de Compatibilidad");

            while (rs.next()) {
                MatricesElegibilidadCompSoloHijo2 lista = new MatricesElegibilidadCompSoloHijo2();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setIdRegla(rs.getString("Id Regla"));
                lista.setTipo(rs.getString("Tipo"));
                lista.setFechaInicio(rs.getDate("Fecha de Inicio"));
                lista.setFechaFinal(rs.getDate("Fecha Final"));
                lista.setAsuntoProducto(rs.getString("Asunto – Producto"));
                lista.setObjetoProducto(rs.getString("Objeto - Producto"));
                lista.setNombrematriz(rs.getString("Nombre Matriz"));
                matricesElegibilidadComp.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Id Regla"), rs.getString("Tipo"), "SE OBTIENEN DATOS HIJO - COMPATIBILIDAD", "MATRICES DE ELEGIBILIDAD Y COMPATIBILIDAD HIJO - COMPATIBILIDAD",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = matricesElegibilidadComp.size();
            Boolean conteo = matricesElegibilidadComp.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Listas de Compatibilidad o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Listas de Compatibilidad o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Listas de Compatibilidad, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Listas de Compatibilidad, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaSoloHijo2, con el error:  " + ex);
            throw ex;
        }
        return matricesElegibilidadComp;
    }

    @Override
    public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick, String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws SiebelException {
        try {
            List<MatricesElegibilidadCompHijo1> hijo = new LinkedList();
            hijo = this.consultaHijo1(IdPadre,usuario,version,ambienteExtra,ambienteInser);
            for (MatricesElegibilidadCompHijo1 sAdminC : hijo) {
                try {
                    BC.activateField("Product");
                    BC.activateField("Type");
                    BC.activateField("Action Code");
                    BC.activateField("TT RPT");
                    BC.activateField("State");
                    BC.activateField("Country");
                    BC.activateField("Adjustment Group Id"); // Id de Padre
                    BC.clearToQuery();
                    BC.setSearchExpr("[Product] ='" + sAdminC.getProducto() + "' AND [TT RPT] = '" + sAdminC.getCiudad() + "'  ");
                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {

                        if (sAdminC.getTipoRegla() != null) {
                            if (!BC.getFieldValue("Type").equals(sAdminC.getTipoRegla())) {
                                oBCPick = BC.getPicklistBusComp("Type");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Value]='" + sAdminC.getTipoRegla() + "'");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getCodigoAccionAsset() != null) {
                            if (!BC.getFieldValue("Action Code").equals(sAdminC.getCodigoAccionAsset())) {
                                BC.setFieldValue("Action Code", sAdminC.getCodigoAccionAsset());
                            }
                        }

                        if (sAdminC.getRegion() != null) {
                            if (!BC.getFieldValue("State").equals(sAdminC.getRegion())) {
                                oBCPick = BC.getPicklistBusComp("State");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Value]='" + sAdminC.getRegion() + "'");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdminC.getPais() != null) {
                            if (!BC.getFieldValue("Country").equals(sAdminC.getPais())) {
                                oBCPick = BC.getPicklistBusComp("Country");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Value]='" + sAdminC.getPais() + "'");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Matrices de Elegibilidad:  " + sAdminC.getProducto() + " : " + sAdminC.getCiudad());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Matrices de Elegibilidad";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getProducto() != null) {
                            oBCPick = BC.getPicklistBusComp("Product");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name]='" + sAdminC.getProducto() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getTipoRegla() != null) {
                            oBCPick = BC.getPicklistBusComp("Type");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value]='" + sAdminC.getTipoRegla() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getCodigoAccionAsset() != null) {
                            BC.setFieldValue("Action Code", sAdminC.getCodigoAccionAsset());
                        }

                        if (sAdminC.getCiudad() != null) {
                            BC.setFieldValue("TT RPT", sAdminC.getCiudad());
                        }

                        if (sAdminC.getRegion() != null) {
                            oBCPick = BC.getPicklistBusComp("State");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value]='" + sAdminC.getRegion() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getPais() != null) {
                            oBCPick = BC.getPicklistBusComp("Country");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value]='" + sAdminC.getPais() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        BC.setFieldValue("Adjustment Group Id", RowID);

                        BC.writeRecord();

                        System.out.println("Se creo Matrices de Elegibilidad:  " + sAdminC.getProducto() + " : " + sAdminC.getCiudad());
                        String mensaje = ("Se creo Matrices de Elegibilidad:  " + sAdminC.getProducto() + " : " + sAdminC.getCiudad());

                        String MsgSalida = "Creado Matrices de Elegibilidad";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);
                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en Hijo : Matriz de Elegibilidad:   " + sAdminC.getProducto() + " : " + sAdminC.getCiudad() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Hijo-Matriz de Elegibilidad";
                    String FlagCarga = "E";
                    String MsgError = "Error en Hijo : Matriz de Elegibilidad:   " + sAdminC.getProducto() + " : " + sAdminC.getCiudad() + "     , con el mensaje:   " + error.replace("'", " ");

                    this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgError);

                }
            }
            //this.CloseDB();
        } catch (SiebelException e) {
            String error = e.getErrorMessage();
            System.out.println("Error en cargaBC, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en cargaBC, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);
        } catch (Exception ex) {
            Logger.getLogger(DAOMatricesElegibilidadCompImpl.class.getName()).log(Level.SEVERE, null, ex);
            //Logger.getLogger(DAOListaPreciosImpl.class.getName()).log(Level.SEVERE, null, ex);
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
    public void cargaBC2(SiebelBusComp BC, SiebelBusComp oBCPick, String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws SiebelException {
        try {
            List<MatricesElegibilidadCompHijo2> hijo = new LinkedList();
            hijo = this.consultaHijo2(IdPadre,usuario,version,ambienteExtra,ambienteInser);
            for (MatricesElegibilidadCompHijo2 sAdminC : hijo) {
                try {
                    BC.activateField("Type");
                    BC.activateField("Effective Start Date");
                    BC.activateField("Effective End Date");
                    BC.activateField("Product"); // Asunto Producto
                    BC.activateField("Rel Product");  // Objeto Producto
                    BC.activateField("Adjustment Group Id"); // Id de Padre
                    BC.clearToQuery();
                    BC.setSearchExpr("[Product] ='" + sAdminC.getAsuntoProducto() + "' AND [Rel Product] = '" + sAdminC.getObjetoProducto() + "'  ");
                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {
                        if (sAdminC.getTipo() != null) {
                            if (!BC.getFieldValue("Type").equals(sAdminC.getTipo())) {
                                oBCPick = BC.getPicklistBusComp("Type");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Value]='" + sAdminC.getTipo() + "' ");
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
                            if (!BC.getFieldValue("Effective Start Date").equals(dateString)) {
                                BC.setFieldValue("Effective Start Date", dateString);
                            }
                        }

                        if (sAdminC.getFechaFinal() != null) {
                            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                            String dateString = format.format(sAdminC.getFechaFinal());
                            if (!BC.getFieldValue("Effective End Date").equals(dateString)) {
                                BC.setFieldValue("Effective End Date", dateString);
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Matrices de Compatibilidad:  " + sAdminC.getAsuntoProducto() + " : " + sAdminC.getObjetoProducto());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Matrices de Compatibilidad";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);
                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getTipo() != null) {
                            oBCPick = BC.getPicklistBusComp("Type");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value]='" + sAdminC.getTipo() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getFechaInicio() != null) {
                            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                            String dateString = format.format(sAdminC.getFechaInicio());
                            BC.setFieldValue("Effective Start Date", dateString);
                        }

                        if (sAdminC.getFechaFinal() != null) {
                            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                            String dateString = format.format(sAdminC.getFechaFinal());
                            BC.setFieldValue("Effective End Date", dateString);
                        }

                        if (sAdminC.getAsuntoProducto() != null) {
                            oBCPick = BC.getPicklistBusComp("Product");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name]='" + sAdminC.getAsuntoProducto() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getObjetoProducto() != null) {
                            oBCPick = BC.getPicklistBusComp("Rel Product");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name]='" + sAdminC.getObjetoProducto() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        BC.setFieldValue("Adjustment Group Id", RowID);

                        BC.writeRecord();

                        System.out.println("Se creo Matrices de Compatibilidad:  " + sAdminC.getAsuntoProducto() + " : " + sAdminC.getObjetoProducto());
                        String mensaje = ("Se creo Matrices de Compatibilidad:  " + sAdminC.getAsuntoProducto() + " : " + sAdminC.getObjetoProducto());

                        String MsgSalida = "Creado Matrices de Compatibilidad";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);
                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en Hijo2 : Matriz de Elegibilidad:   " + sAdminC.getAsuntoProducto() + " : " + sAdminC.getObjetoProducto() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Hijo2-Crear Matriz de Elegibilidad";
                    String FlagCarga = "E";
                    String MsgError = "Error en Hijo2 : Matriz de Elegibilidad:   " + sAdminC.getAsuntoProducto() + " : " + sAdminC.getObjetoProducto() + "     , con el mensaje:   " + error.replace("'", " ");

                    this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgError);

                }
            }
        } catch (SiebelException e) {
            String error = e.getErrorMessage();
            System.out.println("Error en cargaBC2, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en cargaBC2, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);
        } catch (Exception ex) {
            Logger.getLogger(DAOMatricesElegibilidadCompImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void CargaBitacoraEntrada(String Row_Id, String Val1, String Val2, String Seguimiento, String Objeto,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

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

    private void CargaBitacoraSalidaCreado(String rowId, String MsgSalida, String FlagCarga, String MsgError, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

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
