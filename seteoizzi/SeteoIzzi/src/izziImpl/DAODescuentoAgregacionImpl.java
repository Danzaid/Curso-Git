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
import interfaces.DAODescuentoAgregacion;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import objetos.DescuentoAgregacionPadre;
import objetos.DescuentoAgregacionHijo;
import objetos.DescuentoAgregacionSoloHijo;

/**
 *
 * @author moreno.mario
 */
public class DAODescuentoAgregacionImpl extends ConexionDB implements DAODescuentoAgregacion {

    @Override
    public void inserta(List<DescuentoAgregacionPadre> descuentoagregacionpadre, String it, String fechaIni, String fechaTer, String url, String usuarioconn, String passw, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);

        Boolean conteopadre = descuentoagregacionpadre.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI", url, usuarioconn, passw);

            for (DescuentoAgregacionPadre sAdmin : descuentoagregacionpadre) {
                if ("SI".equals(Conectar)) {
                    conSiebel.ConexionSiebel(m_dataBean, Conectar, url, usuarioconn, passw);
                    Conectar = "NO";
                }

                try {

                    String MsgError = "";

                    SiebelBusObject BO = m_dataBean.getBusObject("Pricer Bundle Sequence");
                    SiebelBusComp BC = BO.getBusComp("Pricer Bundle Discount");
                    SiebelBusComp BCCHILD = BO.getBusComp("Pricer Bundle Discount Item");
                    SiebelBusComp oBCPick = new SiebelBusComp();

                    BC.activateField("Name");
                    BC.activateField("Effective From");
                    BC.activateField("Currency Code");
                    BC.activateField("Active Flag");
                    BC.activateField("Description");
                    BC.activateField("Id");
                    BC.clearToQuery();
                    BC.setSearchExpr("[Name] ='" + sAdmin.getNombre() + "'");
                    BC.executeQuery(true);
                    boolean reg = BC.firstRecord();
                    if (reg) {

                        if (sAdmin.getVigentedesde() != null) {
                            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                            String dateString = format.format(sAdmin.getVigentedesde());
                            if (!BC.getFieldValue("Effective From").equals(dateString)) {
                                BC.setFieldValue("Effective From", dateString);
                            }
                        }

                        if (sAdmin.getMoneda() != null) {
                            if (!BC.getFieldValue("Currency Code").equals(sAdmin.getMoneda())) {
                                oBCPick = BC.getPicklistBusComp("Currency Code");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Currency Code] ='" + sAdmin.getMoneda() + "'");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }
                        }

                        if (sAdmin.getActivo() != null) {
                            if (!BC.getFieldValue("Active Flag").equals(sAdmin.getActivo())) {
                                BC.setFieldValue("Active Flag", sAdmin.getActivo());
                            }
                        }

                        if (sAdmin.getComentarios() != null) {
                            if (!BC.getFieldValue("Description").equals(sAdmin.getComentarios())) {
                                BC.setFieldValue("Description", sAdmin.getComentarios());
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Descuento de Agregacion:  " + sAdmin.getNombre());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Descuento de Agregacion";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdmin.getRowId(), MsgSalida, "Y", MsgError, usuario, version, ambienteInser, ambienteExtra);

//                        String RowId1 = BC.getFieldValue("Id");
//                        this.cargaBC(BCCHILD, oBCPick, RowId1, sAdmin.getRowId(), usuario, version, ambienteInser, ambienteExtra);

                        BCCHILD.release();
                        BC.release();
                        BO.release();

                    } else {
                        BC.newRecord(0);

                        if (sAdmin.getNombre() != null) {
                            BC.setFieldValue("Name", sAdmin.getNombre());
                        }

                        if (sAdmin.getVigentedesde() != null) {
                            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                            String dateString = format.format(sAdmin.getVigentedesde());
                            BC.setFieldValue("Effective From", dateString);
                        }

                        if (sAdmin.getMoneda() != null) {
                            oBCPick = BC.getPicklistBusComp("Currency Code");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Currency Code] ='" + sAdmin.getMoneda() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdmin.getActivo() != null) {
                            BC.setFieldValue("Active Flag", sAdmin.getActivo());
                        }

                        if (sAdmin.getComentarios() != null) {
                            BC.setFieldValue("Description", sAdmin.getComentarios());
                        }

                        BC.writeRecord();

                        System.out.println("Se crea registro de Descuento de Agregacion: " + sAdmin.getNombre());
                        String mensaje = ("Se crea registro de Descuento de Agregacion: " + sAdmin.getNombre());
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

                        String MsgSalida = "Creado Descuento de Agregacion";
                        this.CargaBitacoraSalidaCreado(sAdmin.getRowId(), MsgSalida, "Y", MsgError, usuario, version, ambienteInser, ambienteExtra);

//                        String RowID = BC.getFieldValue("Id");
//                        this.cargaBC(BCCHILD, oBCPick, RowID, sAdmin.getRowId(), usuario, version, ambienteInser, ambienteExtra);

                        BCCHILD.release();
                        BC.release();
                        BO.release();

                    }

                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    String error2 = e.getDetailedMessage();
                    System.out.println("Error en creacion Descuento de Agregacion:  " + sAdmin.getNombre() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Descuento de Agregacion";
                    String FlagCarga = "E";
                    String MsgError = "Error en creacion Descuento de Agregacion:  " + sAdmin.getNombre() + "     , con el mensaje:   " + error.replace("'", " ");

                    this.CargaBitacoraSalidaError(sAdmin.getRowId(), MsgSalida, FlagCarga, MsgError, usuario, version, ambienteInser, ambienteExtra);
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
            List<DescuentoAgregacionSoloHijo> hijo = new LinkedList();
            hijo = this.consultaSoloHijo(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra);

            Boolean conteosolohijo = hijo.isEmpty(); // VALIDA SI LA LISTA ESTA VACIA
            if (!conteosolohijo) {

                ConexionSiebel conSiebel = new ConexionSiebel();
                SiebelDataBean m_dataBean = new SiebelDataBean();
                conSiebel.ConexionSiebel(m_dataBean, "SI", url, usuarioconn, passw);

                String IdPadre = null;

                for (DescuentoAgregacionSoloHijo sAdminC : hijo) {  // SI LA LISTA NO ESTA VACIA, COMIENZA EJECUCION
                    if ("SI".equals(Conectar)) {
                        conSiebel.ConexionSiebel(m_dataBean, Conectar, url, usuarioconn, passw);
                        Conectar = "NO";
                    }
                    try {
                        SiebelBusObject BO = m_dataBean.getBusObject("Pricer Bundle Sequence");
                        SiebelBusComp BC = BO.getBusComp("Pricer Bundle Discount");
                        SiebelBusComp BCCHILD = BO.getBusComp("Pricer Bundle Discount Item");
                        SiebelBusComp oBCPick = new SiebelBusComp();

                        BC.activateField("Name");
                        BC.clearToQuery();
                        BC.setSearchExpr("[Name] ='" + sAdminC.getNombredescuento() + "'");  // BUSCA PADRE PARA OBTENER ROW_ID DEL AMBIENTE DESTINO
                        BC.executeQuery(true);
                        boolean reg = BC.firstRecord();
                        if (reg) {
                            IdPadre = BC.getFieldValue("Id");  // OBTIENE ROW_ID DEL AMBIENTE DESTINO

                            BCCHILD.activateField("Sequence");
                            BCCHILD.activateField("Quantity");
                            BCCHILD.activateField("Product Name");
                            BCCHILD.activateField("Required Flag");
                            BCCHILD.activateField("Receive Flag");
                            BCCHILD.activateField("Adjustment Type");
                            BCCHILD.activateField("Adjustment Value");
                            BCCHILD.activateField("Bundle Discount Id"); // Id de Padre
                            BCCHILD.clearToQuery();

                            if (sAdminC.getTipoAjuste() != null) {
                                BCCHILD.setSearchExpr("[Sequence]='" + sAdminC.getSecuencia() + "' AND [Product Name] ='" + sAdminC.getNombreProducto() + "' AND [Adjustment Type]='" + sAdminC.getTipoAjuste() + "' AND [Bundle Discount Id] ='" + IdPadre + "'");
                            } else {
                                BCCHILD.setSearchExpr("[Sequence]='" + sAdminC.getSecuencia() + "' AND [Product Name] ='" + sAdminC.getNombreProducto() + "' AND [Bundle Discount Id] ='" + IdPadre + "'");
                            }

                            BCCHILD.executeQuery(true);
                            boolean regchild = BCCHILD.firstRecord();
                            if (regchild) {

                                if (sAdminC.getCantidad() != null) {
                                    if (!BCCHILD.getFieldValue("Quantity").equals(sAdminC.getCantidad())) {
                                        BCCHILD.setFieldValue("Quantity", sAdminC.getCantidad());
                                    }
                                }

                                if (sAdminC.getComprar() != null) {
                                    if (!BCCHILD.getFieldValue("Required Flag").equals(sAdminC.getComprar())) {
                                        BCCHILD.setFieldValue("Required Flag", sAdminC.getComprar());
                                    }
                                }

                                if (sAdminC.getRecibir() != null) {
                                    if (!BCCHILD.getFieldValue("Receive Flag").equals(sAdminC.getRecibir())) {
                                        BCCHILD.setFieldValue("Receive Flag", sAdminC.getRecibir());
                                    }
                                }

                                if (sAdminC.getImporteAjuste() != null) {
                                    if (!BCCHILD.getFieldValue("Adjustment Value").equals(sAdminC.getImporteAjuste())) {
                                        BCCHILD.setFieldValue("Adjustment Value", sAdminC.getImporteAjuste());
                                    }
                                }

                                BCCHILD.writeRecord();

                                System.out.println("Se valida existencia y/o actualizacion de Hijo - Detalle Descuento de Agregacion:  " + sAdminC.getNombreProducto() + " : " + sAdminC.getTipoAjuste() + " : " + sAdminC.getImporteAjuste());
                                String MsgSalida = "Se valida existencia y/o actualizacion de Hijo - Detalle Descuento de Agregacion";
                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(MsgSalida);

                                this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "", usuario, version, ambienteInser, ambienteExtra);

                                BCCHILD.release();
                                BC.release();
                                BO.release();

                            } else {
                                BCCHILD.newRecord(0);

                                if (sAdminC.getSecuencia() != null) {
                                    BCCHILD.setFieldValue("Sequence", sAdminC.getSecuencia());
                                }

                                if (sAdminC.getCantidad() != null) {
                                    BCCHILD.setFieldValue("Quantity", sAdminC.getCantidad());
                                }

                                if (sAdminC.getNombreProducto() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("Product Name");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Name] ='" + sAdminC.getNombreProducto() + "'");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getComprar() != null) {
                                    BCCHILD.setFieldValue("Required Flag", sAdminC.getComprar());
                                }

                                if (sAdminC.getRecibir() != null) {
                                    BCCHILD.setFieldValue("Receive Flag", sAdminC.getRecibir());
                                }

                                if (sAdminC.getTipoAjuste() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("Adjustment Type");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Value]='" + sAdminC.getTipoAjuste() + "'");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getImporteAjuste() != null) {
                                    BCCHILD.setFieldValue("Adjustment Value", sAdminC.getImporteAjuste());
                                }

                                BCCHILD.setFieldValue("Bundle Discount Id", IdPadre);

                                BCCHILD.writeRecord();

                                System.out.println("Se creo Hijo - Detalle Descuento de Agregacion:  " + sAdminC.getNombreProducto() + " : " + sAdminC.getTipoAjuste() + " : " + sAdminC.getImporteAjuste());
                                String mensaje = ("Se creo Hijo - Detalle Descuento de Agregacion:  " + sAdminC.getNombreProducto() + " : " + sAdminC.getTipoAjuste() + " : " + sAdminC.getImporteAjuste());
                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(mensaje);

                                String MsgSalida = "Creado Hijo - Detalle Descuento de Agregacion";
                                this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "", usuario, version, ambienteInser, ambienteExtra);

                                BCCHILD.release();
                                BC.release();
                                BO.release();

                            }

                        } else {
                            System.out.println("No se encontro registro de Descuento de Agregacion con nombre: " + sAdminC.getNombredescuento() + " , en el ambiente a insertar, por esta razon no puede validarse si existen DETALLES DE DESCUENTO a modificar o crear. ");
                            String MsgSalida = "No se encontro registro de Descuento de Agregacion con nombre: " + sAdminC.getNombredescuento() + " , en el ambiente a insertar, por esta razon no puede validarse si existen DETALLES DE DESCUENTO a modificar o crear. ";
                            String FlagCarga = "E";
                            String MsgError = "No se encontro registro de Descuento de Agregacion con nombre: " + sAdminC.getNombredescuento() + " , en el ambiente a insertar, por esta razon no puede validarse si existen DETALLES DE DESCUENTO a modificar o crear. ";

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgError);
                            this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError, usuario, version, ambienteInser, ambienteExtra);

                            BCCHILD.release();
                            BC.release();
                            BO.release();

                        }

                    } catch (SiebelException e) {
                        String error = e.getErrorMessage();
                        System.out.println("Error en creacion de Hijo - Detalle Descuento de Agregacion:  " + sAdminC.getNombreProducto() + " : " + sAdminC.getTipoAjuste() + " : " + sAdminC.getImporteAjuste() + "     , con el mensaje:   " + error.replace("'", " "));
                        String MsgSalida = "Error al Crear Hijo - Detalle Descuento de Agregacion";
                        String FlagCarga = "E";
                        String MsgError = "Error en creacion de Hijo - Detalle Descuento de Agregacion:  " + sAdminC.getNombreProducto() + " : " + sAdminC.getTipoAjuste() + " : " + sAdminC.getImporteAjuste() + "     , con el mensaje:   " + error.replace("'", " ");

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
            System.out.println("Error en  validacion Solo Hijo - Detalle Descuento de Agregacion:  " + error.replace("'", " "));
            String MsgError = "Error en  validacion Solo Hijo - Detalle Descuento de Agregacion:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);

        }
    }

    @Override
    public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick, String RowID, String IdPadre, String usuario, String version, String ambienteInser, String ambienteExtra) throws SiebelException {
        try {
            List<DescuentoAgregacionHijo> hijo = new LinkedList();
            hijo = this.consultaHijo(IdPadre, usuario, version, ambienteInser, ambienteExtra);
            for (DescuentoAgregacionHijo sAdminC : hijo) {
                try {
                    BC.activateField("Sequence");
                    BC.activateField("Quantity");
                    BC.activateField("Product Name");
                    BC.activateField("Required Flag");
                    BC.activateField("Receive Flag");
                    BC.activateField("Adjustment Type");
                    BC.activateField("Adjustment Value");
                    BC.activateField("Bundle Discount Id"); // Id de Padre
                    BC.clearToQuery();

                    if (sAdminC.getTipoAjuste() != null) {
                        BC.setSearchExpr("[Sequence]='" + sAdminC.getSecuencia() + "' AND [Product Name] ='" + sAdminC.getNombreProducto() + "' AND [Adjustment Type]='" + sAdminC.getTipoAjuste() + "'");
                    } else {
                        BC.setSearchExpr("[Sequence]='" + sAdminC.getSecuencia() + "' AND [Product Name] ='" + sAdminC.getNombreProducto() + "'");
                    }

                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {

                        if (sAdminC.getCantidad() != null) {
                            if (!BC.getFieldValue("Quantity").equals(sAdminC.getCantidad())) {
                                BC.setFieldValue("Quantity", sAdminC.getCantidad());
                            }
                        }

                        if (sAdminC.getComprar() != null) {
                            if (!BC.getFieldValue("Required Flag").equals(sAdminC.getComprar())) {
                                BC.setFieldValue("Required Flag", sAdminC.getComprar());
                            }
                        }

                        if (sAdminC.getRecibir() != null) {
                            if (!BC.getFieldValue("Receive Flag").equals(sAdminC.getRecibir())) {
                                BC.setFieldValue("Receive Flag", sAdminC.getRecibir());
                            }
                        }

                        if (sAdminC.getImporteAjuste() != null) {
                            if (!BC.getFieldValue("Adjustment Value").equals(sAdminC.getImporteAjuste())) {
                                BC.setFieldValue("Adjustment Value", sAdminC.getImporteAjuste());
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Hijo - Detalle Descuento de Agregacion:  " + sAdminC.getNombreProducto() + " : " + sAdminC.getTipoAjuste() + " : " + sAdminC.getImporteAjuste());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Hijo - Detalle Descuento de Agregacion";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "", usuario, version, ambienteInser, ambienteExtra);

                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getSecuencia() != null) {
                            BC.setFieldValue("Sequence", sAdminC.getSecuencia());
                        }

                        if (sAdminC.getCantidad() != null) {
                            BC.setFieldValue("Quantity", sAdminC.getCantidad());
                        }

                        if (sAdminC.getNombreProducto() != null) {
                            oBCPick = BC.getPicklistBusComp("Product Name");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name] ='" + sAdminC.getNombreProducto() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getComprar() != null) {
                            BC.setFieldValue("Required Flag", sAdminC.getComprar());
                        }

                        if (sAdminC.getRecibir() != null) {
                            BC.setFieldValue("Receive Flag", sAdminC.getRecibir());
                        }

                        if (sAdminC.getTipoAjuste() != null) {
                            oBCPick = BC.getPicklistBusComp("Adjustment Type");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Value]='" + sAdminC.getTipoAjuste() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getImporteAjuste() != null) {
                            BC.setFieldValue("Adjustment Value", sAdminC.getImporteAjuste());
                        }

                        BC.setFieldValue("Bundle Discount Id", RowID);

                        BC.writeRecord();

                        System.out.println("Se creo Hijo - Detalle Descuento de Agregacion:  " + sAdminC.getNombreProducto() + " : " + sAdminC.getTipoAjuste() + " : " + sAdminC.getImporteAjuste());
                        String mensaje = ("Se creo Hijo - Detalle Descuento de Agregacion:  " + sAdminC.getNombreProducto() + " : " + sAdminC.getTipoAjuste() + " : " + sAdminC.getImporteAjuste());
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

                        String MsgSalida = "Creado Hijo - Detalle Descuento de Agregacion";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "", usuario, version, ambienteInser, ambienteExtra);

                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    String error2 = e.getDetailedMessage();
                    System.out.println("Error en Hijo - Detalle Descuento de Agregacion:" + sAdminC.getNombreProducto() + " : " + sAdminC.getTipoAjuste() + " : " + sAdminC.getImporteAjuste() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Hijo - Detalle Descuento de Agregacion";
                    String FlagCarga = "E";
                    String MsgError = "Error en Hijo - Detalle Descuento de Agregacion:" + sAdminC.getNombreProducto() + " : " + sAdminC.getTipoAjuste() + " : " + sAdminC.getImporteAjuste() + "     , con el mensaje:   " + error.replace("'", " ");

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
    public List<DescuentoAgregacionPadre> consultaPadre(String fechaI, String fechaT, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.BUNDLE_DISCNT_NAME \"Nombre\", A.EFF_START_DT \"Vigente Desde\", A.CURCY_CD \"Moneda\", A.ACTIVE_FLG \"Activo\", A.DESC_TEXT \"Comentarios\"\n"
                + "FROM SIEBEL.S_BUNDLE_DISCNT A, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<DescuentoAgregacionPadre> descuentoagregacionpadre = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {
            System.out.println("Creando Lista de registros Padre a procesar.");
            while (rs.next()) {
                DescuentoAgregacionPadre lista = new DescuentoAgregacionPadre();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setVigentedesde(rs.getDate("Vigente Desde"));
                lista.setMoneda(rs.getString("Moneda"));
                lista.setActivo(rs.getString("Activo"));
                lista.setComentarios(rs.getString("Comentarios"));
                descuentoagregacionpadre.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre"), "", "", "SE OBTIENEN DATOS", "DESCUENTOS DE AGREGACION", usuario, version, ambienteInser, ambienteExtra);

            }
            int Registros = descuentoagregacionpadre.size();
            Boolean conteo = descuentoagregacionpadre.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Descuentos de Agregacion o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Descuentos de Agregacion o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Descuentos de Agregacion, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Descuentos de Agregacion, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return descuentoagregacionpadre;
    }

    @Override
    public List<DescuentoAgregacionHijo> consultaHijo(String IdPadre, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.BUNDLE_DISCNT_ID \"Id Padre\",B.ITEM_SEQ_NUM \"Secuencia\", B.ITEM_QTY \"Cantidad\", C.NAME \"Nombre de Producto\", B.REQUIRED_FLG \"Comprar\", B.PRI_ADJ_TYPE_CD \"Tipo de Ajuste\",B.ADJ_VAL_AMT \"Importe del Ajuste\"\n"
                + "FROM SIEBEL.S_BDL_DISC_ITEM B, SIEBEL.S_PROD_INT C, SIEBEL.S_USER H\n"
                + "WHERE B.PROD_ID = C.ROW_ID(+) AND B.LAST_UPD_BY = H.ROW_ID\n"
                + "AND B.BUNDLE_DISCNT_ID = '" + IdPadre + "' AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')"
                + "ORDER BY B.CREATED ASC";
        List<DescuentoAgregacionHijo> descuentoagregacionhijo = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar.");

            while (rs.next()) {
                DescuentoAgregacionHijo lista = new DescuentoAgregacionHijo();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setSecuencia(rs.getString("Secuencia"));
                lista.setCantidad(rs.getString("Cantidad"));
                lista.setNombreProducto(rs.getString("Nombre de Producto"));
                lista.setComprar(rs.getString("Comprar"));
                lista.setTipoAjuste(rs.getString("Tipo de Ajuste"));
                lista.setImporteAjuste(rs.getString("Importe del Ajuste"));

                descuentoagregacionhijo.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Secuencia"), rs.getString("Cantidad"), rs.getString("Nombre de Producto"), "SE OBTIENEN DATOS HIJO", "DESCUENTOS DE AGREGACION - DETALLE", usuario, version, ambienteInser, ambienteExtra);

            }
            int Registros = descuentoagregacionhijo.size();
            Boolean conteo = descuentoagregacionhijo.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Descuentos de Agregacion o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Descuentos de Agregacion o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Descuentos de Agregacion, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Descuentos de Agregacion, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo, con el error:  " + ex);
            throw ex;
        }
        return descuentoagregacionhijo;

    }

    @Override
    public List<DescuentoAgregacionSoloHijo> consultaSoloHijo(String fechaI, String fechaT, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.BUNDLE_DISCNT_ID \"Id Padre\",B.ITEM_SEQ_NUM \"Secuencia\", B.ITEM_QTY \"Cantidad\", C.NAME \"Nombre de Producto\", B.REQUIRED_FLG \"Comprar\", B.PRI_ADJ_TYPE_CD \"Tipo de Ajuste\",\n"
                + "B.ADJ_VAL_AMT \"Importe del Ajuste\", f.BUNDLE_DISCNT_NAME \"Nombre Descuento\"\n"
                + "FROM SIEBEL.S_BDL_DISC_ITEM B, SIEBEL.S_PROD_INT C, SIEBEL.S_BUNDLE_DISCNT F,SIEBEL.S_USER H\n"
                + "WHERE B.PROD_ID = C.ROW_ID(+) AND B.BUNDLE_DISCNT_ID (+)= F.ROW_ID AND B.LAST_UPD_BY = H.ROW_ID\n"
                + "AND B.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN = '" + usuario + "' AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA E WHERE E.ROW_ID = B.ROW_ID AND E.USUARIO ='" + usuario + "' AND E.VERSION ='" + version + "' AND E.EXTRAER ='" + ambienteExtra + "' AND E.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY B.LAST_UPD ASC";
        List<DescuentoAgregacionSoloHijo> descuentoagregacionhijo = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Solo Hijos a procesar.");

            while (rs.next()) {
                DescuentoAgregacionSoloHijo lista = new DescuentoAgregacionSoloHijo();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setSecuencia(rs.getString("Secuencia"));
                lista.setCantidad(rs.getString("Cantidad"));
                lista.setNombreProducto(rs.getString("Nombre de Producto"));
                lista.setComprar(rs.getString("Comprar"));
                lista.setTipoAjuste(rs.getString("Tipo de Ajuste"));
                lista.setImporteAjuste(rs.getString("Importe del Ajuste"));
                lista.setIdpadre(rs.getString("Id Padre"));
                lista.setNombredescuento(rs.getString("Nombre Descuento"));
                descuentoagregacionhijo.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Secuencia"), rs.getString("Cantidad"), rs.getString("Nombre de Producto"), "SE OBTIENEN DATOS HIJO", "DESCUENTOS DE AGREGACION - DETALLE", usuario, version, ambienteInser, ambienteExtra);

            }
            int Registros = descuentoagregacionhijo.size();
            Boolean conteo = descuentoagregacionhijo.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Descuentos de Agregacion o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Descuentos de Agregacion o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Descuentos de Agregacion, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Descuentos de Agregacion, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaSoloHijo, con el error:  " + ex);
            throw ex;
        }
        return descuentoagregacionhijo;

    }

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
