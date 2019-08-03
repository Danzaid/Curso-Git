/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package izziImpl;

import com.siebel.data.SiebelBusObject;
import com.siebel.data.SiebelBusComp;
import com.siebel.data.SiebelDataBean;
import com.siebel.data.SiebelException;
import dao.ConexionDB;
import dao.ConexionSiebel;
import dao.Reportes;
import interfaces.DAOListaPrecios;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import objetos.ListaPreciosHijo;
import objetos.ListaPreciosPadre;
import objetos.ListaPreciosSoloHijo;

/**
 *
 * @author Felipe Gutierrez
 */
public class DAOListaPreciosImpl extends ConexionDB implements DAOListaPrecios {

    @Override
    public void inserta(List<ListaPreciosPadre> listaPrecios, String it, String fechaIni, String fechaTer, String url, String usuarioconn, String passw, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);

        Boolean conteopadre = listaPrecios.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI", url, usuarioconn, passw);

            for (ListaPreciosPadre sAdminC : listaPrecios) {
                if ("SI".equals(Conectar)) {
                    conSiebel.ConexionSiebel(m_dataBean, Conectar, url, usuarioconn, passw);
                    Conectar = "NO";
                }

                try {

                    String MsgError = "";

                    SiebelBusObject BO = m_dataBean.getBusObject("Admin Price List");
                    SiebelBusObject BO2 = m_dataBean.getBusObject("Admin ISS Product Definition");
                    SiebelBusComp BC = BO.getBusComp("Price List");
                    SiebelBusComp BCCHILD = BO.getBusComp("Price List Item");
                    SiebelBusComp BCCHILD2 = BO2.getBusComp("Internal Product");
                    SiebelBusComp oBCPick = new SiebelBusComp();

                    BC.activateField("Name");
                    BC.activateField("Description");
                    BC.clearToQuery();
                    BC.setSearchExpr("[Name] ='" + sAdminC.getNombre() + "' ");
                    BC.executeQuery(true);
                    boolean reg = BC.firstRecord();
                    if (reg) {
                        if (sAdminC.getDescripcion() != null) {
                            // ACTUALIZA PADRE

                            if (!BC.getFieldValue("Description").equals(sAdminC.getDescripcion())) {
                                BC.setFieldValue("Description", sAdminC.getDescripcion());
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Listas de Precios :  " + sAdminC.getNombre());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Listas de Precios";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowId(), MsgSalida, "Y", MsgError, usuario, version, ambienteInser, ambienteExtra);

                        // CONSULTA HIJOS
//                        String RowId1 = BC.getFieldValue("Id");
//                        this.cargaBC(BCCHILD, BCCHILD2, oBCPick, RowId1, sAdminC.getRowId(),usuario,version,ambienteInser,ambienteExtra);
                        BCCHILD.release();
                        BC.release();
                        BO.release();

                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getNombre() != null) {
                            BC.setFieldValue("Name", sAdminC.getNombre());
                        }

                        if (sAdminC.getDescripcion() != null) {
                            BC.setFieldValue("Description", sAdminC.getDescripcion());
                        }

                        BC.writeRecord();

                        System.out.println("Se crea registro de Listas de Precios: " + sAdminC.getNombre());
                        String mensaje = ("Se crea registro de Listas de Precios: " + sAdminC.getNombre());

                        String MsgSalida = "Creado Listas de Precios";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowId(), MsgSalida, "Y", MsgError, usuario, version, ambienteInser, ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

                        // BUSCA Y SETEA HIJOS
//                        String RowID = BC.getFieldValue("Id");
//                        this.cargaBC(BCCHILD, BCCHILD2, oBCPick, RowID, sAdminC.getRowId(),usuario,version,ambienteInser,ambienteExtra);
                        BCCHILD.release();
                        BC.release();
                        BO.release();

                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en creacion Listas de Precios:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Listas de Precios";
                    String FlagCarga = "E";
                    String MsgError = "Error en creacion Listas de Precios:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " ");

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

        try {                                                                       // BUSCA REGISTROS NUEVOS O ACTUALIZADOS, DESDE EL HIJO 
            List<ListaPreciosSoloHijo> hijo = new LinkedList();
            hijo = this.consultaSoloHijo(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra);

            Boolean conteosolohijo = hijo.isEmpty(); // VALIDA SI LA LISTA ESTA VACIA
            if (!conteosolohijo) {

                ConexionSiebel conSiebel = new ConexionSiebel();
                SiebelDataBean m_dataBean = new SiebelDataBean();
                conSiebel.ConexionSiebel(m_dataBean, "SI", url, usuarioconn, passw);

                String IdPadre = null;

                for (ListaPreciosSoloHijo sAdminC : hijo) {  // SI LA LISTA NO ESTA VACIA, COMIENZA EJECUCION
                    if ("SI".equals(Conectar)) {
                        conSiebel.ConexionSiebel(m_dataBean, Conectar, url, usuarioconn, passw);
                        Conectar = "NO";
                    }
                    try {
                        SiebelBusObject BO = m_dataBean.getBusObject("Admin Price List");
                        SiebelBusObject BO2 = m_dataBean.getBusObject("Admin ISS Product Definition");
                        SiebelBusComp BC = BO.getBusComp("Price List");
                        SiebelBusComp BCCHILD = BO.getBusComp("Price List Item");
                        SiebelBusComp BCCHILD2 = BO2.getBusComp("Internal Product");
                        SiebelBusComp oBCPick = new SiebelBusComp();

                        BC.activateField("Name");
                        BC.clearToQuery();
                        BC.setSearchExpr("[Name] ='" + sAdminC.getNombrelistapadre() + "'");  // BUSCA PADRE PARA OBTENER ROW_ID DEL AMBIENTE DESTINO
                        BC.executeQuery(true);
                        boolean reg = BC.firstRecord();
                        if (reg) {
                            IdPadre = BC.getFieldValue("Id");  // OBTIENE ROW_ID DEL AMBIENTE DESTINO

                            // SI SE ENCONTRO ROW_ID DE PADRE, COMIENZA BUSQUEDA DE HIJOS 1 - CONDICIONES
                            BCCHILD.activateField("Product Name");
                            BCCHILD.activateField("Original List Price");
                            BCCHILD.activateField("CV Portal One Time Discount %");
                            BCCHILD.activateField("CV Portal Recurrent Discount %");
                            BCCHILD.activateField("Price List Id"); // Id de Padre
                            BCCHILD.clearToQuery();
                            BCCHILD.setSearchExpr("[Product Name] ='" + sAdminC.getNombre() + "' AND [Price List Id] ='" + IdPadre + "'");
                            BCCHILD.executeQuery(true);
                            boolean regchild = BCCHILD.firstRecord();
                            if (regchild) {
                                if (sAdminC.getPreciolista() != null) {
                                    if (!BCCHILD.getFieldValue("Original List Price").equals(sAdminC.getPreciolista())) {
                                        BCCHILD.setFieldValue("Original List Price", sAdminC.getPreciolista());
                                    }
                                }

                                if (sAdminC.getDescuentoUnicaVez() != null) {
                                    if (!BCCHILD.getFieldValue("CV Portal One Time Discount %").equals(sAdminC.getDescuentoUnicaVez())) {
                                        BCCHILD.setFieldValue("CV Portal One Time Discount %", sAdminC.getDescuentoUnicaVez());
                                    }
                                }

                                if (sAdminC.getRecurrentdiscount() != null) {
                                    if (!BCCHILD.getFieldValue("CV Portal Recurrent Discount %").equals(sAdminC.getRecurrentdiscount())) {
                                        BCCHILD.setFieldValue("CV Portal Recurrent Discount %", sAdminC.getRecurrentdiscount());
                                    }
                                }

                                BCCHILD.writeRecord();

                                System.out.println("Se valida existencia y/o actualizacion de Hijo - Detalle de Lista de Precios:  " + sAdminC.getNombre() + ", hijo de la Lista de Precios: " + sAdminC.getNombrelistapadre());
                                String MsgSalida = "Se valida existencia y/o actualizacion de Hijo - Detalle de Lista de Precios:  " + sAdminC.getNombre() + ", hijo de la Lista de Precios: " + sAdminC.getNombrelistapadre();
                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(MsgSalida);

                                this.CargaBitacoraSalidaValidado(sAdminC.getRowId(), MsgSalida, "Y", "", usuario, version, ambienteInser, ambienteExtra);

                                BCCHILD.release();
                                BC.release();
                                BO.release();

                            } else {

                                String CreaRegistro = this.ValidaProducto(BCCHILD2, sAdminC.getNombre());

                                if ("SI".equals(CreaRegistro)) {
                                    BCCHILD.newRecord(0);

                                    if (sAdminC.getNombre() != null) {
                                        oBCPick = BCCHILD.getPicklistBusComp("Product Name");
                                        oBCPick.clearToQuery();
                                        oBCPick.setSearchExpr("[Name]='" + sAdminC.getNombre() + "' ");
                                        oBCPick.executeQuery(true);
                                        if (oBCPick.firstRecord()) {
                                            oBCPick.pick();
                                        }
                                        oBCPick.release();
                                    }

                                    if (sAdminC.getPreciolista() != null) {
                                        BCCHILD.setFieldValue("Original List Price", sAdminC.getPreciolista());
                                    }

                                    if (sAdminC.getDescuentoUnicaVez() != null) {
                                        BCCHILD.setFieldValue("CV Portal One Time Discount %", sAdminC.getDescuentoUnicaVez());
                                    }

                                    if (sAdminC.getRecurrentdiscount() != null) {
                                        BCCHILD.setFieldValue("CV Portal Recurrent Discount %", sAdminC.getRecurrentdiscount());
                                    }

                                    BCCHILD.setFieldValue("Price List Id", IdPadre);
                                    BCCHILD.writeRecord();

                                    System.out.println("Se creo Item de Hijo - Detalle de Lista de Precios:  " + sAdminC.getNombre() + " , que pertenece a la lista de precios: " + sAdminC.getNombrelistapadre());
                                    String mensaje = ("Se creo Item de Hijo - Detalle de Lista de Precios:  " + sAdminC.getNombre() + " , que pertenece a la lista de precios: " + sAdminC.getNombrelistapadre());

                                    String MsgSalida = "Creado Item de Hijo - Detalle de Lista de Precios";
                                    this.CargaBitacoraSalidaCreado(sAdminC.getRowId(), MsgSalida, "Y", "", usuario, version, ambienteInser, ambienteExtra);

                                    Reportes rep = new Reportes();
                                    rep.agregarTextoAlfinal(mensaje);
                                } else {

                                    System.out.println("No se puede crear el registro de Detalle ya el que producto: " + sAdminC.getNombre() + " , no existe en el ambiente de Insercion");
                                    String MsgSalida = "No se puede crear el registro de Detalle ya el que producto: " + sAdminC.getNombre() + " , no existe en el ambiente de Insercion";
                                    String FlagCarga = "E";
                                    String MsgError = "No se puede crear el registro de Detalle ya el que producto: " + sAdminC.getNombre() + " , no existe en el ambiente de Insercion";

                                    Reportes rep = new Reportes();
                                    rep.agregarTextoAlfinal(MsgError);

                                    this.CargaBitacoraSalidaError(sAdminC.getRowId(), MsgSalida, FlagCarga, MsgError, usuario, version, ambienteInser, ambienteExtra);
                                }

                                BCCHILD.release();
                                BC.release();
                                BO.release();

                            }

                        } else {
                            System.out.println("No se encontro registro de Lista de Precio con nombre: " + sAdminC.getNombrelistapadre() + " , en el ambiente a insertar, por esta razon no puede validarse si existen DETALLE a modificar o crear. ");
                            String MsgSalida = "No se encontro registro de Lista de Precio con nombre: " + sAdminC.getNombrelistapadre() + " , en el ambiente a insertar, por esta razon no puede validarse si existen DETALLE a modificar o crear. ";
                            String FlagCarga = "E";
                            String MsgError = "No se encontro registro de Lista de Precio con nombre: " + sAdminC.getNombrelistapadre() + " , en el ambiente a insertar, por esta razon no puede validarse si existen DETALLE a modificar o crear. ";

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgError);
                            this.CargaBitacoraSalidaError(sAdminC.getRowId(), MsgSalida, FlagCarga, MsgError, usuario, version, ambienteInser, ambienteExtra);

                            BCCHILD.release();
                            BC.release();
                            BO.release();
                        }

                    } catch (SiebelException e) {
                        String error = e.getErrorMessage();
                        System.out.println("Error en creacion de Detalle de Lista de Precios:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " "));
                        String MsgSalida = "Error al Crear Detalle de Lista de Precios";
                        String FlagCarga = "E";
                        String MsgError = "Error en creacion de Detalle de Lista de Precios:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " ");

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgError);

                        this.CargaBitacoraSalidaError(sAdminC.getRowId(), MsgSalida, FlagCarga, MsgError, usuario, version, ambienteInser, ambienteExtra);

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
            System.out.println("Error en  validacion Solo Hijo - Lista de Precios, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en  validacion Solo Hijo - Lista de Precios, con el error:  " + error.replace("'", " ");
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
    public void cargaBC(SiebelBusComp BC, SiebelBusComp BC2, SiebelBusComp oBCPick, String RowID, String IdPadre, String usuario, String version, String ambienteInser, String ambienteExtra) throws SiebelException {
        try {
            List<ListaPreciosHijo> hijo = new LinkedList();
            hijo = this.consultaHijo(IdPadre, usuario, version, ambienteInser, ambienteExtra);
            for (ListaPreciosHijo sAdminC : hijo) {
                try {
                    BC.activateField("Product Name");
                    BC.activateField("Original List Price");
                    BC.activateField("CV Portal One Time Discount %");
                    BC.activateField("CV Portal Recurrent Discount %");
                    BC.activateField("Price List Id"); // Id de Padre
                    BC.clearToQuery();
                    BC.setSearchSpec("Product Name", sAdminC.getNombre());
                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {
                        if (sAdminC.getPreciolista() != null) {
                            if (!BC.getFieldValue("Original List Price").equals(sAdminC.getPreciolista())) {
                                BC.setFieldValue("Original List Price", sAdminC.getPreciolista());
                            }
                        }

                        if (sAdminC.getDescuentoUnicaVez() != null) {
                            if (!BC.getFieldValue("CV Portal One Time Discount %").equals(sAdminC.getDescuentoUnicaVez())) {
                                BC.setFieldValue("CV Portal One Time Discount %", sAdminC.getDescuentoUnicaVez());
                            }
                        }

                        if (sAdminC.getRecurrentdiscount() != null) {
                            if (!BC.getFieldValue("CV Portal Recurrent Discount %").equals(sAdminC.getRecurrentdiscount())) {
                                BC.setFieldValue("CV Portal Recurrent Discount %", sAdminC.getRecurrentdiscount());
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Detalle de Lista de Precios:  " + sAdminC.getNombre());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Detalle de Lista de Precios";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowId(), MsgSalida, "Y", "", usuario, version, ambienteInser, ambienteExtra);

                    } else {

                        String CreaRegistro = this.ValidaProducto(BC2, sAdminC.getNombre());

                        if ("SI".equals(CreaRegistro)) {
                            BC.newRecord(0);

                            if (sAdminC.getNombre() != null) {
                                oBCPick = BC.getPicklistBusComp("Product Name");
                                oBCPick.clearToQuery();
                                oBCPick.setSearchExpr("[Name]='" + sAdminC.getNombre() + "' ");
                                oBCPick.executeQuery(true);
                                if (oBCPick.firstRecord()) {
                                    oBCPick.pick();
                                }
                                oBCPick.release();
                            }

                            if (sAdminC.getPreciolista() != null) {
                                BC.setFieldValue("Original List Price", sAdminC.getPreciolista());
                            }

                            if (sAdminC.getDescuentoUnicaVez() != null) {
                                BC.setFieldValue("CV Portal One Time Discount %", sAdminC.getDescuentoUnicaVez());
                            }

                            if (sAdminC.getRecurrentdiscount() != null) {
                                BC.setFieldValue("CV Portal Recurrent Discount %", sAdminC.getRecurrentdiscount());
                            }

                            BC.setFieldValue("Price List Id", RowID);
                            BC.writeRecord();

                            System.out.println("Se creo Item de Lista de Precios:  " + sAdminC.getNombre());
                            String mensaje = ("Se creo Item de Lista de Precios:  " + sAdminC.getNombre());

                            String MsgSalida = "Creado Item de Lista de Precios";
                            this.CargaBitacoraSalidaCreado(sAdminC.getRowId(), MsgSalida, "Y", "", usuario, version, ambienteInser, ambienteExtra);

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(mensaje);
                        } else {
                            System.out.println("No se puede crear el registro de Detalle ya el que producto: " + sAdminC.getNombre() + " , no existe en el ambiente de Insercion");
                            String MsgSalida = "No se puede crear el registro de Detalle ya el que producto: " + sAdminC.getNombre() + " , no existe en el ambiente de Insercion";
                            String FlagCarga = "E";
                            String MsgError = "No se puede crear el registro de Detalle ya el que producto: " + sAdminC.getNombre() + " , no existe en el ambiente de Insercion";

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgError);

                            this.CargaBitacoraSalidaError(sAdminC.getRowId(), MsgSalida, FlagCarga, MsgError, usuario, version, ambienteInser, ambienteExtra);

                        }

                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error al crear Detalle de Lista de Precios:" + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Detalle de Lista de Precios";
                    String FlagCarga = "E";
                    String MsgError = "Error al crear Detalle de Lista de Precios:" + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " ");
                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgError);

                    this.CargaBitacoraSalidaError(sAdminC.getRowId(), MsgSalida, FlagCarga, MsgError, usuario, version, ambienteInser, ambienteExtra);

                }
            }
        } catch (SiebelException e) {
            String error = e.getErrorMessage();
            System.out.println("Error en cargaBC, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en cargaBC, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);
        } catch (Exception ex) {
            Logger.getLogger(DAOListaPreciosImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public List<ListaPreciosPadre> consultaPadre(String fechaI, String fechaT, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.NAME \"Nombre\", A.DESC_TEXT \"Descripcion\"\n"
                + "FROM SIEBEL.S_PRI_LST A, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<ListaPreciosPadre> listaPrecios = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Padre a procesar.");

            while (rs.next()) {
                ListaPreciosPadre lista = new ListaPreciosPadre();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setDescripcion(rs.getString("Descripcion"));
                listaPrecios.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre"), "", "SE OBTIENEN DATOS", "LISTAS DE PRECIOS", usuario, version, ambienteInser, ambienteExtra);

            }
            int Registros = listaPrecios.size();
            Boolean conteo = listaPrecios.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Padres de Lista de Precios o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Padres de Lista de Precios o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Padres de Lista de Precios, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Padres de Lista de Precios, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return listaPrecios;
    }

    @Override
    public List<ListaPreciosHijo> consultaHijo(String IdPadre, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.PRI_LST_ID \"Id Padre\", B.PROD_ID, B.PROD_NAME \"Nombre\", B.STD_PRI_UNIT \"Precio de lista\", \n"
                + "B.X_ONE_TIME_DISCOUNT \"% Descuento única Vez\", B.X_RECURRENT_DISCOUNT \"Recurrent Discount %\"\n"
                + "FROM SIEBEL.S_PRI_LST_ITEM B, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = B.LAST_UPD_BY\n"
                + "AND B.PRI_LST_ID = '" + IdPadre + "' AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')"
                + "ORDER BY B.LAST_UPD ASC";
        List<ListaPreciosHijo> listaPrecios = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar.");

            while (rs.next()) {
                ListaPreciosHijo lista = new ListaPreciosHijo();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setPreciolista(rs.getString("Precio de lista"));
                lista.setDescuentoUnicaVez(rs.getString("% Descuento única Vez"));
                lista.setRecurrentdiscount(rs.getString("Recurrent Discount %"));
                listaPrecios.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre"), "", "SE OBTIENEN DATOS HIJO", "LISTAS DE PRECIOS - DETALLE", usuario, version, ambienteInser, ambienteExtra);

            }
            int Registros = listaPrecios.size();
            Boolean conteo = listaPrecios.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Detalle - Lista de Precios o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Detalle Lista de Precios o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Detalle - Lista de Precios, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Detalle Lista de Precios, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo, con el error:  " + ex);
            throw ex;
        }
        return listaPrecios;
    }

    @Override
    public List<ListaPreciosSoloHijo> consultaSoloHijo(String fechaI, String fechaT, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.PRI_LST_ID \"Id Padre\", B.PROD_ID, B.PROD_NAME \"Nombre\", B.STD_PRI_UNIT \"Precio de lista\", \n"
                + "B.X_ONE_TIME_DISCOUNT \"% Descuento única Vez\", B.X_RECURRENT_DISCOUNT \"Recurrent Discount %\", A.NAME \"Nombre Lista\"\n"
                + "FROM SIEBEL.S_PRI_LST_ITEM B, SIEBEL.S_PRI_LST A,SIEBEL.S_USER H\n"
                + "WHERE B.PRI_LST_ID = A.ROW_ID AND H.ROW_ID = B.LAST_UPD_BY\n"
                + "AND B.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN = '" + usuario + "' AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA E WHERE E.ROW_ID = B.ROW_ID AND E.USUARIO ='" + usuario + "' AND E.VERSION ='" + version + "' AND E.EXTRAER ='" + ambienteExtra + "' AND E.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY B.LAST_UPD ASC";
        List<ListaPreciosSoloHijo> listaPreciossolohijo = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar.");

            while (rs.next()) {
                ListaPreciosSoloHijo lista = new ListaPreciosSoloHijo();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setPreciolista(rs.getString("Precio de lista"));
                lista.setDescuentoUnicaVez(rs.getString("% Descuento única Vez"));
                lista.setRecurrentdiscount(rs.getString("Recurrent Discount %"));
                lista.setNombrelistapadre(rs.getString("Nombre Lista"));
                listaPreciossolohijo.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre"), "", "SE OBTIENEN DATOS HIJO", "LISTAS DE PRECIOS - DETALLE", usuario, version, ambienteInser, ambienteExtra);

            }
            int Registros = listaPreciossolohijo.size();
            Boolean conteo = listaPreciossolohijo.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Detalle de Lista de Precios o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Detalle de Lista de Precios o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Detalle de Lista de Precios, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Detalle de Lista de Precios, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaSoloHijo, con el error:  " + ex);
            throw ex;
        }
        return listaPreciossolohijo;
    }

    private void CargaBitacoraEntrada(String Row_Id, String Val1, String Val2, String Seguimiento, String Objeto, String usuario, String version, String ambienteInser, String ambienteExtra) throws Exception {

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

    private String ValidaProducto(SiebelBusComp BCCHILD2, String nombre) throws Exception {

        String Crea = "NO";
        BCCHILD2.activateField("Price List Id"); // Id de Padre
        BCCHILD2.clearToQuery();
        BCCHILD2.setSearchExpr("[Name]='" + nombre + "'");
        BCCHILD2.executeQuery(true);
        boolean regchild = BCCHILD2.firstRecord();
        if (regchild) {
            Crea = "SI";
        }
        return (Crea);

    }

}
