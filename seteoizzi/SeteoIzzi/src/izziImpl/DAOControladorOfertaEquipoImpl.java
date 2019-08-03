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
import interfaces.DAOControladorOfertaEquipo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import objetos.ControladorOfertaEquipoHijo1;
import objetos.ControladorOfertaEquipoHijo2;
import objetos.ControladorOfertaEquipoPadre;
import objetos.ControladorOfertaEquipoSoloHijo1;
import objetos.ControladorOfertaEquipoSoloHijo2;

/**
 *
 * @author hector.pineda
 */
public class DAOControladorOfertaEquipoImpl extends ConexionDB implements DAOControladorOfertaEquipo {

    @Override
    public void inserta(List<ControladorOfertaEquipoPadre> controladorOfertaEquipo, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);

        Boolean conteopadre = controladorOfertaEquipo.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

            for (ControladorOfertaEquipoPadre sAdminC : controladorOfertaEquipo) {
                if ("SI".equals(Conectar)) {
                    conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                    Conectar = "NO";
                }

                try {

                    String MsgError = "";

                    SiebelBusObject BO = m_dataBean.getBusObject("TT Item Management");
                    SiebelBusComp BC = BO.getBusComp("TT Item Mgmt");
                    SiebelBusComp BCCHILD = BO.getBusComp("TT Item Zone");
                    SiebelBusComp BCCHILD2 = BO.getBusComp("TT Equipment Group");
                    SiebelBusComp oBCPick = new SiebelBusComp();

                    BC.activateField("Name");
                    BC.activateField("Product Part");
                    BC.activateField("Product Desc");
                    BC.clearToQuery();
                    BC.setSearchExpr("[Name] ='" + sAdminC.getItem() + "' ");
                    BC.executeQuery(true);
                    boolean reg = BC.firstRecord();
                    if (reg) {
                        // SIN ACCIONES                    
                        System.out.println("Se valida existencia y/o actualizacion de Controlador Oferta Equipo:  " + sAdminC.getItem());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Controlador Oferta Equipo";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

//                        String RowId1 = BC.getFieldValue("Id");
//                        this.cargaBC(BCCHILD, BCCHILD2, oBCPick, RowId1, sAdminC.getRowId(),usuario,version,ambienteInser,ambienteExtra);

                        BCCHILD2.release();
                        BCCHILD.release();
                        BC.release();
                        BO.release();
                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getItem() != null) {
                            oBCPick = BC.getPicklistBusComp("Name");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name]='" + sAdminC.getItem() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        BC.writeRecord();

                        System.out.println("Se crea registro de Controlador Oferta Equipo: " + sAdminC.getItem());
                        String mensaje = ("Se crea registro de Controlador Oferta Equipo: " + sAdminC.getItem());

                        String MsgSalida = "Creado Controlador Oferta Equipo";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowId(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

//                        String RowID = BC.getFieldValue("Id");
//                        this.cargaBC(BCCHILD, BCCHILD2, oBCPick, RowID, sAdminC.getRowId(),usuario,version,ambienteInser,ambienteExtra);

                        BCCHILD2.release();
                        BCCHILD.release();
                        BC.release();
                        BO.release();

                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en creacion Controlador Oferta Equipo:  " + sAdminC.getItem() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Controlador Oferta Equipo";
                    String FlagCarga = "E";
                    String MsgError = "Error en creacion Controlador Oferta Equipo:  " + sAdminC.getItem() + "     , con el mensaje:   " + error.replace("'", " ");

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

    try {                                                                       // BUSCA REGISTROS NUEVOS O ACTUALIZADOS, DESDE EL HIJO 1 - ZONAS
        List<ControladorOfertaEquipoSoloHijo1> hijo = new LinkedList();
        hijo = this.consultaSoloHijo1(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra);

        Boolean conteosolohijo = hijo.isEmpty(); // VALIDA SI LA LISTA ESTA VACIA
        if (!conteosolohijo) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

            for (ControladorOfertaEquipoSoloHijo1 sAdminC : hijo) {  // SI LA LISTA NO ESTA VACIA, COMIENZA EJECUCION
                if ("SI".equals(Conectar)) {
                    conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                    Conectar = "NO";
                }
                try {
                    SiebelBusObject BO = m_dataBean.getBusObject("TT Item Management");
                    SiebelBusComp BC = BO.getBusComp("TT Item Mgmt");
                    SiebelBusComp BCCHILD = BO.getBusComp("TT Item Zone");
                    SiebelBusComp oBCPick = new SiebelBusComp();

                    BC.activateField("Name");
                    BC.clearToQuery();
                    BC.setSearchExpr("[Name] ='" + sAdminC.getNombrecontrolador() + "' ");  // BUSCA PADRE PARA OBTENER ROW_ID DEL AMBIENTE DESTINO
                    BC.executeQuery(true);
                    boolean reg = BC.firstRecord();
                    if (reg) {
                        String IdPadre = BC.getFieldValue("Id");  // OBTIENE ROW_ID DEL AMBIENTE DESTINO

                        // SI SE ENCONTRO ROW_ID DE PADRE, COMIENZA BUSQUEDA DE HIJOS 1 - CONDICIONES
                        BCCHILD.activateField("Name");
                        BCCHILD.activateField("Primary Item Id"); // Id de Padre
                        BCCHILD.clearToQuery();
                        BCCHILD.setSearchExpr("[Name]='" + sAdminC.getZona() + "' AND [Primary Item Id]= '" + IdPadre + "'");
                        BCCHILD.executeQuery(true);
                        boolean regchild = BCCHILD.firstRecord();
                        if (regchild) {
                            // SIN ACCION
                            System.out.println("Se valida existencia y/o actualizacion de Zonas:  " + sAdminC.getZona());
                            String MsgSalida = "Se valida existencia y/o actualizacion de Zonas";
                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgSalida);

                            this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                            BCCHILD.release();
                            BC.release();
                            BO.release();

                        } else {
                            BCCHILD.newRecord(0);

                            oBCPick = BCCHILD.getPicklistBusComp("Name");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name]='" + sAdminC.getZona() + "'");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();

                            BCCHILD.setFieldValue("Primary Item Id", IdPadre);

                            BCCHILD.writeRecord();

                            System.out.println("Se creo Zona:  " + sAdminC.getZona());
                            String mensaje = ("Se creo Zona:  " + sAdminC.getZona());

                            String MsgSalida = "Creado Zona";
                            this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(mensaje);

                            BCCHILD.release();
                            BC.release();
                            BO.release();
                        }
//                            IdPadre = null;

                    } else {
                        System.out.println("No se encontro registro de Controlador Oferta Equipo con nombre: " + sAdminC.getNombrecontrolador() + " , en el ambiente a insertar, por esta razon no puede validarse si existen ZONAS a modificar o crear. ");
                        String MsgSalida = "No se encontro registro de Controlador Oferta Equipo para cear Zona";
                        String FlagCarga = "E";
                        String MsgError = "No se encontro registro de Controlador Oferta Equipo con nombre: " + sAdminC.getNombrecontrolador() + " , en el ambiente a insertar, por esta razon no puede validarse si existen ZONAS a modificar o crear. ";

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgError);
                        this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                        BCCHILD.release();
                        BC.release();
                        BO.release();
                    }

                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en creacion de Zona:  " + sAdminC.getZona() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Zona";
                    String FlagCarga = "E";
                    String MsgError = "Error en creacion de Zona:  " + sAdminC.getZona() + "     , con el mensaje:   " + error.replace("'", " ");

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
        System.out.println("Error en  validacion Hijo - Zonas, con el error:  " + error.replace("'", " "));
        String MsgError = "Error en  validacion Hijo - Zonas, con el error:  " + error.replace("'", " ");
        Reportes rep = new Reportes();
        rep.agregarTextoAlfinal(MsgError);

    }

        
    
    try {                                                                       // BUSCA REGISTROS NUEVOS O ACTUALIZADOS, DESDE EL NIETO 1 - Grupos de Eq. Permitidos
            List<ControladorOfertaEquipoSoloHijo2> hijo = new LinkedList();
            hijo = this.consultaSoloHijo2(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra);

            Boolean conteosolohijo2 = hijo.isEmpty(); // VALIDA SI LA LISTA ESTA VACIA
            if (!conteosolohijo2) {

                ConexionSiebel conSiebel = new ConexionSiebel();
                SiebelDataBean m_dataBean = new SiebelDataBean();
                conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

                for (ControladorOfertaEquipoSoloHijo2 sAdminC : hijo) {  // SI LA LISTA NO ESTA VACIA, COMIENZA EJECUCION
                    if ("SI".equals(Conectar)) {
                        conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                        Conectar = "NO";
                    }
                    try {
                        SiebelBusObject BO = m_dataBean.getBusObject("TT Item Management");
                        SiebelBusComp BC = BO.getBusComp("TT Item Mgmt");
                        SiebelBusComp BCCHILD = BO.getBusComp("TT Item Zone");
                        SiebelBusComp BCCHILD2 = BO.getBusComp("TT Equipment Group");
                        SiebelBusComp oBCPick = new SiebelBusComp();

                        BC.activateField("Name");
                        BC.clearToQuery();
                        BC.setSearchExpr("[Name] ='" + sAdminC.getNombrecontrolador() + "' ");  // BUSCA PADRE PARA OBTENER ROW_ID DEL "ABUELO", EN AMBIENTE DESTINO
                        BC.executeQuery(true);
                        boolean reg = BC.firstRecord();
                        if (reg) {
                            String IdAbue = BC.getFieldValue("Id");  // OBTIENE ROW_ID DEL "ABUELO" EN AMBIENTE DESTINO

                            BCCHILD.activateField("Name");
                            BCCHILD.activateField("Primary Item Id");
                            BCCHILD.clearToQuery();
                            BCCHILD.setSearchExpr("[Name] ='" + sAdminC.getNombrezona() + "' AND [Primary Item Id]= '" + IdAbue + "'");  // BUSCA PADRE PARA OBTENER ROW_ID DEL PADRE EN AMBIENTE DESTINO
                            BCCHILD.executeQuery(true);
                            boolean regchild1 = BCCHILD.firstRecord();
                            if (regchild1) {
                                String IdPadre = BCCHILD.getFieldValue("Id");  // OBTIENE ROW_ID DEL "PADRE" EN AMBIENTE DESTINO

                                // SI SE ENCONTRO ROW_ID DE PADRE, COMIENZA BUSQUEDA DE Grupos de Eq. Permitidos
                                BCCHILD2.activateField("Name");
                                BCCHILD2.activateField("Zone Id"); // Id de Padre
                                BCCHILD2.clearToQuery();
                                BCCHILD2.setSearchExpr("[Name]='" + sAdminC.getGrupoEquipos() + "' AND [Zone Id]= '" + IdPadre + "'");
                                BCCHILD2.executeQuery(true);
                                boolean regchild2 = BCCHILD2.firstRecord();
                                if (regchild2) {
                                    // SIN ACCION

                                    System.out.println("Se valida existencia y/o actualizacion de Grupos de Eq. Permitidos:  " + sAdminC.getGrupoEquipos());
                                    String MsgSalida = "Se valida existencia y/o actualizacion de Grupos de Eq. Permitidos";
                                    Reportes rep = new Reportes();
                                    rep.agregarTextoAlfinal(MsgSalida);

                                    this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);
                             
                                    BCCHILD2.release();
                                    BCCHILD.release();
                                    BC.release();
                                    BO.release();

                                } else {
                                    BCCHILD2.newRecord(0);

                                    oBCPick = BCCHILD2.getPicklistBusComp("Name");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Value]='" + sAdminC.getGrupoEquipos() + "' ");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();

                                    BCCHILD2.setFieldValue("Zone Id", IdPadre);

                                    BCCHILD2.writeRecord();

                                    System.out.println("Se creo Grupos de Eq. Permitidos:  " + sAdminC.getGrupoEquipos());
                                    String mensaje = ("Se creo Grupos de Eq. Permitidos:  " + sAdminC.getGrupoEquipos());

                                    String MsgSalida = "Creado Grupos de Eq. Permitidos";
                                    this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                    Reportes rep = new Reportes();
                                    rep.agregarTextoAlfinal(mensaje);

                                    BCCHILD2.release();
                                    BCCHILD.release();
                                    BC.release();
                                    BO.release();

                                }
                            } else {
                                System.out.println("No se encontro registro de Zona con nombre: " + sAdminC.getNombrezona() + " , en el ambiente a insertar, por esta razon no puede validarse si existen GRUPO DE EQ. PERMITIDOS a modificar o crear. ");
                                String MsgSalida = "No se encontro registro de Zona para validar si existen GRUPO DE EQ. PERMITIDOS.";
                                String FlagCarga = "E";
                                String MsgError = "No se encontro registro de Zona con nombre: " + sAdminC.getNombrezona() + " , en el ambiente a insertar, por esta razon no puede validarse si existen GRUPO DE EQ. PERMITIDOS a modificar o crear. ";

                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(MsgError);
                                this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                                BCCHILD2.release();
                                BCCHILD.release();
                                BC.release();
                                BO.release();
                            }

                        } else {
                            System.out.println("No se encontro registro de Controlador Oferta Equipo con nombre: " + sAdminC.getNombrecontrolador() + " , en el ambiente a insertar, por esta razon no puede validarse si existen GRUPO DE EQ. PERMITIDOS a modificar o crear. ");
                            String MsgSalida = "No se encontro registro de Controlador Oferta Equipo psra crear GRUPO DE EQ. PERMITIDOS";
                            String FlagCarga = "E";
                            String MsgError = "No se encontro registro de Controlador Oferta Equipo con nombre: " + sAdminC.getNombrecontrolador() + " , en el ambiente a insertar, por esta razon no puede validarse si existen GRUPO DE EQ. PERMITIDOS a modificar o crear. ";

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgError);
                            this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                            BCCHILD2.release();
                            BCCHILD.release();
                            BC.release();
                            BO.release();
                        }

                    } catch (SiebelException e) {
                        String error = e.getErrorMessage();
                        System.out.println("Error en creacion de Grupos de Eq. Permitidos:  " + sAdminC.getNombrezona() + "     , con el mensaje:   " + error.replace("'", " "));
                        String MsgSalida = "Error al Crear Grupos de Eq. Permitidos";
                        String FlagCarga = "E";
                        String MsgError = "Error en creacion de Grupos de Eq. Permitidos:  " + sAdminC.getNombrezona() + "     , con el mensaje:   " + error.replace("'", " ");

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
            System.out.println("Error en  validacion Hijo - Grupos de Eq. Permitidos, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en  validacion Hijo - Grupos de Eq. Permitidos, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);

        }

    }
    
    

    @Override
    public List<ControladorOfertaEquipoPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.NAME \"Item\", B.PART_NUM \"No de Pieza\", B.DESC_TEXT \"Descripción de producto\"\n"
                + "FROM SIEBEL.CX_MGMT_ITEM A, SIEBEL.S_PROD_INT B, SIEBEL.S_USER H\n"
                + "WHERE A.PROD_ID  = B.ROW_ID (+) AND H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.TYPE = 'Item'\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<ControladorOfertaEquipoPadre> controladorOfertaEquipo = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Padre a procesar.");

            while (rs.next()) {
                ControladorOfertaEquipoPadre lista = new ControladorOfertaEquipoPadre();
                lista.setRowId(rs.getString("ROW_ID"));
                lista.setNoPieza(rs.getString("No de Pieza"));
                lista.setDescripcionProducto(rs.getString("Descripción de producto"));
                lista.setItem(rs.getString("Item"));

                controladorOfertaEquipo.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Item"), rs.getString("No de Pieza"), "SE OBTIENEN DATOS", "CONTROLADOR OFERTA EQUIPO",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = controladorOfertaEquipo.size();
            Boolean conteo = controladorOfertaEquipo.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas Padre de Controlador Oferta Equipo o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Padre de Controlador Oferta Equipo o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas Padre de Controlador Oferta Equipo, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas Padre de Controlador Oferta Equipo, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return controladorOfertaEquipo;
    }

    @Override
    public List<ControladorOfertaEquipoHijo1> consultaHijo1(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.ITEM_ID \"Id Padre\", B.NAME \"Zona\"\n"
                + "FROM SIEBEL.CX_MGMT_ITEM B, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = B.LAST_UPD_BY\n"
                + "AND B.TYPE = 'Zona'\n"
                + "AND B.ITEM_ID = '" + IdPadre + "'\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')"
                + "ORDER BY B.LAST_UPD ASC";
        List<ControladorOfertaEquipoHijo1> controladorOfertaEquipo = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar - Zonas");

            while (rs.next()) {
                ControladorOfertaEquipoHijo1 lista = new ControladorOfertaEquipoHijo1();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setZona(rs.getString("Zona"));
                controladorOfertaEquipo.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Zona"), "", "SE OBTIENEN DATOS HIJO", "CONTROLADOR OFERTA EQUIPO - ZONAS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = controladorOfertaEquipo.size();
            Boolean conteo = controladorOfertaEquipo.isEmpty(); // Valida si la lista esta vacia
            String mensaje;
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Zonas o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Zonas o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Zonas, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Zonas, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo1, con el error:  " + ex);
            throw ex;
        }
        return controladorOfertaEquipo;
    }

    @Override
    public List<ControladorOfertaEquipoSoloHijo1> consultaSoloHijo1(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.ITEM_ID \"Id Padre\", B.NAME \"Zona\", A.NAME \"Nombre Controlador\"\n"
                + "FROM SIEBEL.CX_MGMT_ITEM B, SIEBEL.CX_MGMT_ITEM A, SIEBEL.S_USER H\n"
                + "WHERE B.ITEM_ID = A.ROW_ID AND H.ROW_ID = B.LAST_UPD_BY\n"
                + "AND  B.TYPE = 'Zona'\n"
                + "AND B.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY B.LAST_UPD ASC";
        List<ControladorOfertaEquipoSoloHijo1> controladorOfertaEquiposolohijo1 = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar - Zonas");

            while (rs.next()) {
                ControladorOfertaEquipoSoloHijo1 lista = new ControladorOfertaEquipoSoloHijo1();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setZona(rs.getString("Zona"));
                lista.setNombrecontrolador(rs.getString("Nombre Controlador"));
                controladorOfertaEquiposolohijo1.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Zona"), "", "SE OBTIENEN DATOS HIJO", "CONTROLADOR OFERTA EQUIPO HIJO - ZONAS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = controladorOfertaEquiposolohijo1.size();
            Boolean conteo = controladorOfertaEquiposolohijo1.isEmpty(); // Valida si la lista esta vacia
            String mensaje;
            if (conteo) {
                System.out.println("No se obtuvieron listas de Solo Hijos - Zonas o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Solo Hijos Zonas o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Solo Hijos - Zonas, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Solo Hijos Zonas, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaSoloHijo1, con el error:  " + ex);
            throw ex;
        }
        return controladorOfertaEquiposolohijo1;
    }

    @Override
    public List<ControladorOfertaEquipoHijo2> consultaHijo2(String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.ZONE_ID \"Id Zona Padre\", B.ITEM_ID \"Id Item\", B.NAME \"Grupo de Equipos\"\n"
                + "FROM SIEBEL.CX_MGMT_ITEM B, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = B.LAST_UPD_BY\n"
                + "AND B.TYPE = 'Grupo'\n"
                + "AND B.ZONE_ID = '" + IdPadre + "'\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')"
                + "ORDER BY B.LAST_UPD ASC";
        List<ControladorOfertaEquipoHijo2> controladorOfertaEquipo = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar - Gpo. Equipos");

            while (rs.next()) {
                ControladorOfertaEquipoHijo2 lista = new ControladorOfertaEquipoHijo2();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setGrupoEquipos(rs.getString("Grupo de Equipos"));
                controladorOfertaEquipo.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Grupo de Equipos"), "", "SE OBTIENEN DATOS HIJO", "CONTROLADOR OFERTA EQUIPO - GPO. EQUIPOS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = controladorOfertaEquipo.size();
            Boolean conteo = controladorOfertaEquipo.isEmpty(); // Valida si la lista esta vacia
            String mensaje;
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Gpo. Equipos o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Gpo. Equipos o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Gpo. Equipos, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Gpo. Equipos, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo2, con el error:  " + ex);
            throw ex;
        }
        return controladorOfertaEquipo;
    }

    @Override
    public List<ControladorOfertaEquipoSoloHijo2> consultaSoloHijo2(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.ZONE_ID \"Id Zona Padre\", B.ITEM_ID \"Id Item\", B.NAME \"Grupo de Equipos\", C.NAME \"Nombre Zona\",  A.NAME \"Nombre Controlador\"\n"
                + "FROM SIEBEL.CX_MGMT_ITEM B, SIEBEL.CX_MGMT_ITEM C, SIEBEL.CX_MGMT_ITEM A, SIEBEL.S_USER H\n"
                + "WHERE B.ZONE_ID = C.ROW_ID AND C.ITEM_ID = A.ROW_ID AND H.ROW_ID = B.LAST_UPD_BY\n"
                + "AND B.TYPE = 'Grupo'\n"
                + "AND B.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY B.LAST_UPD ASC";
        List<ControladorOfertaEquipoSoloHijo2> controladorOfertaEquiposolohijo2 = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar - Gpo. Equipos");

            while (rs.next()) {
                ControladorOfertaEquipoSoloHijo2 lista = new ControladorOfertaEquipoSoloHijo2();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setGrupoEquipos(rs.getString("Grupo de Equipos"));
                lista.setNombrezona(rs.getString("Nombre Zona"));
                lista.setNombrecontrolador(rs.getString("Nombre Controlador"));
                controladorOfertaEquiposolohijo2.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Grupo de Equipos"), "", "SE OBTIENEN DATOS HIJO", "CONTROLADOR OFERTA EQUIPO - GPO. EQUIPOS",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = controladorOfertaEquiposolohijo2.size();
            Boolean conteo = controladorOfertaEquiposolohijo2.isEmpty(); // Valida si la lista esta vacia
            String mensaje;
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Gpo. Equipos o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Gpo. Equipos o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Gpo. Equipos, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Gpo. Equipos, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaSoloHijo2, con el error:  " + ex);
            throw ex;
        }
        return controladorOfertaEquiposolohijo2;
    }

    @Override
    public void cargaBC(SiebelBusComp BC, SiebelBusComp BC2, SiebelBusComp oBCPick, String RowID, String IdPadre,String usuario,String version,String ambienteInser,String ambienteExtra) throws SiebelException {
        try {
            List<ControladorOfertaEquipoHijo1> hijo = new LinkedList();
            hijo = this.consultaHijo1(IdPadre,usuario,version,ambienteInser,ambienteExtra);
            for (ControladorOfertaEquipoHijo1 sAdminC : hijo) {
                try {
                    BC.activateField("Name");
                    BC.activateField("Primary Item Id"); // Id de Padre
                    BC.clearToQuery();
                    BC.setSearchExpr("[Name]='" + sAdminC.getZona() + "' AND [Primary Item Id]= '" + RowID + "'");
                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {
                        // SIN ACCION
                        System.out.println("Se valida existencia y/o actualizacion de Zonas:  " + sAdminC.getZona());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Zonas";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                        String RowId1 = BC.getFieldValue("Id");
                        this.cargaBC2(BC2, oBCPick, RowId1, sAdminC.getRowid(),usuario,version,ambienteInser,ambienteExtra);

                    } else {
                        BC.newRecord(0);

                        oBCPick = BC.getPicklistBusComp("Name");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Name]='" + sAdminC.getZona() + "'");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();

                        BC.setFieldValue("Primary Item Id", RowID);

                        BC.writeRecord();

                        System.out.println("Se creo Zona:  " + sAdminC.getZona());
                        String mensaje = ("Se creo Zona:  " + sAdminC.getZona());

                        String MsgSalida = "Creado Zona";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

                        String RowID2 = BC.getFieldValue("Id");
                        this.cargaBC2(BC2, oBCPick, RowID2, sAdminC.getRowid(),usuario,version,ambienteInser,ambienteExtra);
                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en Zona:" + sAdminC.getZona() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error en Zona:" + sAdminC.getZona() + "     , con el mensaje:   " + error.replace("'", " ");
                    String FlagCarga = "E";
                    String MsgError = "Error en Zona:" + sAdminC.getZona() + "     , con el mensaje:   " + error.replace("'", " ");

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
            Logger.getLogger(DAOControladorOfertaEquipoImpl.class.getName()).log(Level.SEVERE, null, ex);
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
            List<ControladorOfertaEquipoHijo2> hijo = new LinkedList();
            hijo = this.consultaHijo2(IdPadre,usuario,version,ambienteInser,ambienteExtra);
            for (ControladorOfertaEquipoHijo2 sAdminC : hijo) {
                try {
                    BC.activateField("Name");
                    BC.activateField("Zone Id"); // Id de Padre
                    BC.clearToQuery();
                    BC.setSearchExpr("[Name]='" + sAdminC.getGrupoEquipos() + "' AND [Zone Id]= '" + RowID + "'");
                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {
                        // SIN ACCION

                        System.out.println("Se valida existencia y/o actualizacion de Grupos de Eq. Permitidos:  " + sAdminC.getGrupoEquipos());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Grupos de Eq. Permitidos";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                    } else {
                        BC.newRecord(0);

                        oBCPick = BC.getPicklistBusComp("Name");
                        oBCPick.clearToQuery();
                        oBCPick.setSearchExpr("[Value]='" + sAdminC.getGrupoEquipos() + "' ");
                        oBCPick.executeQuery(true);
                        if (oBCPick.firstRecord()) {
                            oBCPick.pick();
                        }
                        oBCPick.release();

                        BC.setFieldValue("Zone Id", RowID);

                        BC.writeRecord();

                        System.out.println("Se creo Grupos de Eq. Permitidos:  " + sAdminC.getGrupoEquipos());
                        String mensaje = ("Se creo Grupos de Eq. Permitidos:  " + sAdminC.getGrupoEquipos());

                        String MsgSalida = "Creado Grupos de Eq. Permitidos";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    String error2 = e.getDetailedMessage();
                    System.out.println("Error en Grupos de Eq. Permitidos:" + sAdminC.getGrupoEquipos() + "     , con el mensaje:   " + error2.replace("'", " "));
                    String MsgSalida = "Error al Crear Grupos de Eq. Permitidos";
                    String FlagCarga = "E";
                    String MsgError = "Error en Grupos de Eq. Permitidos:" + sAdminC.getGrupoEquipos() + "     , con el mensaje:   " + error2.replace("'", " ");

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
            Logger.getLogger(DAOControladorOfertaEquipoImpl.class.getName()).log(Level.SEVERE, null, ex);
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
