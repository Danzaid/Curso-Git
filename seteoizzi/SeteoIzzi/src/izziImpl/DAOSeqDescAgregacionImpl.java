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
import interfaces.DAOSeqDescAgregacion;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import objetos.SeqDescAgregacionHijo;
import objetos.SeqDescAgregacionPadre;
import objetos.SeqDescAgregacionSoloHijo;

/**
 *
 * @author Felipe Gutierrez
 */
public class DAOSeqDescAgregacionImpl extends ConexionDB implements DAOSeqDescAgregacion {

    @Override
    public void inserta(List<SeqDescAgregacionPadre> seqDescAgregacion, String it, String fechaIni, String fechaTer,String url,String usuarioconn,String passw, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

        String Conectar = "NO";
        int Contador = 0;
        int Maxima = Integer.parseInt(it);

        Boolean conteopadre = seqDescAgregacion.isEmpty(); // Valida si la lista esta vacia
        if (!conteopadre) {

            ConexionSiebel conSiebel = new ConexionSiebel();
            SiebelDataBean m_dataBean = new SiebelDataBean();
            conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

            for (SeqDescAgregacionPadre sLista : seqDescAgregacion) {
                if ("SI".equals(Conectar)) {
                    conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                    Conectar = "NO";
                }

                try {

                    String MsgError = "";

                    SiebelBusObject BO = m_dataBean.getBusObject("Pricer Bundle Sequence");
                    SiebelBusComp BC = BO.getBusComp("Pricer Bundle Sequence");
                    SiebelBusComp BCCHILD = BO.getBusComp("Pricer Bundle Sequence Item");
                    SiebelBusComp oBCPick = new SiebelBusComp();

                    BC.activateField("Name");
                    BC.activateField("Effective From");
                    BC.activateField("Effective To");
                    BC.activateField("Active Flag");
                    BC.activateField("Description");
                    BC.clearToQuery();
                    BC.setSearchExpr("[Name]='" + sLista.getNombre() + "' ");
                    BC.executeQuery(true);
                    boolean reg = BC.firstRecord();
                    if (reg) {
                        if (sLista.getVijenteDesde() != null) {
                            if (!BC.getFieldValue("Effective From").equals(sLista.getVijenteDesde())) {
                                SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                                String dateString = format.format(sLista.getVijenteDesde());
                                BC.setFieldValue("Effective From", dateString);
                            }
                        }

                        if (sLista.getVijenteHasta() != null) {
                            if (!BC.getFieldValue("Effective To").equals(sLista.getVijenteHasta())) {
                                SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                                String dateString = format.format(sLista.getVijenteHasta());
                                BC.setFieldValue("Effective To", dateString);
                            }
                        }

                        if (sLista.getActivo() != null) {
                            if (!BC.getFieldValue("Active Flag").equals(sLista.getActivo())) {
                                BC.setFieldValue("Active Flag", sLista.getActivo());
                            }
                        }

                        if (sLista.getComentario() != null) {
                            if (!BC.getFieldValue("Description").equals(sLista.getComentario())) {
                                BC.setFieldValue("Description", sLista.getComentario());
                            }
                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Seq. Descuento Agragacion:  " + sLista.getNombre());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Seq. Descuento Agragacion";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sLista.getRowid(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

//                        String RowId1 = BC.getFieldValue("Id");
//                        this.cargaBC(BCCHILD, oBCPick, RowId1, sLista.getRowid(),usuario,version,ambienteInser,ambienteExtra);

                        BCCHILD.release();
                        BC.release();
                        BO.release();

                    } else {
                        BC.newRecord(0);

                        if (sLista.getNombre() != null) {
                            BC.setFieldValue("Name", sLista.getNombre());
                        }

                        if (sLista.getVijenteDesde() != null) {
                            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                            String dateString = format.format(sLista.getVijenteDesde());
                            BC.setFieldValue("Effective From", dateString);
                        }

                        if (sLista.getVijenteHasta() != null) {
                            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                            String dateString = format.format(sLista.getVijenteHasta());
                            BC.setFieldValue("Effective To", dateString);
                        }

                        if (sLista.getActivo() != null) {
                            BC.setFieldValue("Active Flag", sLista.getActivo());
                        }

                        if (sLista.getComentario() != null) {
                            BC.setFieldValue("Description", sLista.getComentario());
                        }

                        BC.writeRecord();

                        System.out.println("Se crea registro de Seq. Descuento Agragacion: " + sLista.getNombre());
                        String mensaje = ("Se crea registro de Seq. Descuento Agragacion: " + sLista.getNombre());
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);

                        String MsgSalida = "Creado Seq. Descuento Agragacion";
                        this.CargaBitacoraSalidaCreado(sLista.getRowid(), MsgSalida, "Y", MsgError,usuario,version,ambienteInser,ambienteExtra);

//                        String RowID = BC.getFieldValue("Id");
//                        this.cargaBC(BCCHILD, oBCPick, RowID, sLista.getRowid(),usuario,version,ambienteInser,ambienteExtra);

                        BCCHILD.release();
                        BC.release();
                        BO.release();

                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en creacion Seq. Descuento Agregacion:  " + sLista.getNombre() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error al Crear Seq. Descuento Agregacion";
                    String FlagCarga = "E";
                    String MsgError = "Error en creacion Seq. Descuento Agregacion:  " + sLista.getNombre() + "     , con el mensaje:   " + error.replace("'", " ");
                    Reportes rep = new Reportes();
                    rep.agregarTextoAlfinal(MsgError);

                    this.CargaBitacoraSalidaError(sLista.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

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
            List<SeqDescAgregacionSoloHijo> hijo = new LinkedList();
            hijo = this.consultaSoloHijo(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra);

            Boolean conteosolohijo = hijo.isEmpty(); // VALIDA SI LA LISTA ESTA VACIA
            if (!conteosolohijo) {

                ConexionSiebel conSiebel = new ConexionSiebel();
                SiebelDataBean m_dataBean = new SiebelDataBean();
                conSiebel.ConexionSiebel(m_dataBean, "SI",url,usuarioconn,passw);

                String IdPadre = null;

                for (SeqDescAgregacionSoloHijo sAdminC : hijo) {  // SI LA LISTA NO ESTA VACIA, COMIENZA EJECUCION
                    if ("SI".equals(Conectar)) {
                        conSiebel.ConexionSiebel(m_dataBean, Conectar,url,usuarioconn,passw);
                        Conectar = "NO";
                    }
                    try {
                        SiebelBusObject BO = m_dataBean.getBusObject("Pricer Bundle Sequence");
                        SiebelBusComp BC = BO.getBusComp("Pricer Bundle Sequence");
                        SiebelBusComp BCCHILD = BO.getBusComp("Pricer Bundle Sequence Item");
                        SiebelBusComp oBCPick = new SiebelBusComp();

                        BC.activateField("Name");
                        BC.clearToQuery();
                        BC.setSearchExpr("[Name]='" + sAdminC.getNombreseqDescAgreg() + "' ");
                        BC.executeQuery(true);
                        boolean reg = BC.firstRecord();
                        if (reg) {
                            IdPadre = BC.getFieldValue("Id");  // OBTIENE ROW_ID DEL PADRE EN EL AMBIENTE DESTINO
                            
                            // SI SE ENCONTRO ROW_ID DE PADRE, COMIENZA BUSQUEDA DE HIJOS 1 - CONDICIONES
                            
                            BCCHILD.activateField("Bundle Discount Name");
                            BCCHILD.activateField("Next Seq If True");
                            BCCHILD.activateField("Next Seq If False");
                            BCCHILD.activateField("Sequence");
                            BCCHILD.activateField("Bundle Sequence Id"); // Id Padre
                            BCCHILD.clearToQuery();
                            BCCHILD.setSearchExpr("[Bundle Discount Name]='" + sAdminC.getNombre() + "' AND [Sequence]= '" + sAdminC.getSecuencia() + "' AND [Next Seq If False] = '" + sAdminC.getSigDescuentoNoUtiliza() + "' AND [Bundle Sequence Id] = '" + IdPadre + "'");
                            BCCHILD.executeQuery(true);
                            boolean regchild = BCCHILD.firstRecord();
                            if (regchild) {
                                if (sAdminC.getSigDescuentoSiUtiliza() != null) {
                                    if (!BCCHILD.getFieldValue("Next Seq If True").equals(sAdminC.getSigDescuentoSiUtiliza())) {
                                        BCCHILD.setFieldValue("Next Seq If True", sAdminC.getSigDescuentoSiUtiliza());
                                    }
                                }

//                                if (sAdminC.getSigDescuentoNoUtiliza() != null) {
//                                    if (!BCCHILD.getFieldValue("Next Seq If False").equals(sAdminC.getSigDescuentoNoUtiliza())) {
//                                        BCCHILD.setFieldValue("Next Seq If False", sAdminC.getSigDescuentoNoUtiliza());
//                                    }
//                                }

                                BCCHILD.writeRecord();

                                System.out.println("Se valida existencia y/o actualizacion de Item Hijo - Descuento de Agregacion:  " + sAdminC.getNombre() + " ,del padre: " + sAdminC.getNombreseqDescAgreg());
                                String MsgSalida = "Se valida existencia y/o actualizacion de Item Hijo - Descuento de Agregacion" + sAdminC.getNombre() + " ,del padre: " + sAdminC.getNombreseqDescAgreg();
                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(MsgSalida);

                                this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                BCCHILD.release();
                                BC.release();
                                BO.release();

                            } else {
                                BCCHILD.newRecord(0);

                                if (sAdminC.getSecuencia() != null) {
                                    BCCHILD.setFieldValue("Sequence", sAdminC.getSecuencia());
                                }

                                if (sAdminC.getNombre() != null) {
                                    oBCPick = BCCHILD.getPicklistBusComp("Bundle Discount Name");
                                    oBCPick.clearToQuery();
                                    oBCPick.setSearchExpr("[Name]='" + sAdminC.getNombre() + "' ");
                                    oBCPick.executeQuery(true);
                                    if (oBCPick.firstRecord()) {
                                        oBCPick.pick();
                                    }
                                    oBCPick.release();
                                }

                                if (sAdminC.getSigDescuentoSiUtiliza() != null) {
                                    BCCHILD.setFieldValue("Next Seq If True", sAdminC.getSigDescuentoSiUtiliza());
                                }

                                if (sAdminC.getSigDescuentoNoUtiliza() != null) {
                                    BCCHILD.setFieldValue("Next Seq If False", sAdminC.getSigDescuentoNoUtiliza());
                                }

                                BCCHILD.setFieldValue("Bundle Sequence Id", IdPadre);

                                BCCHILD.writeRecord();

                                System.out.println("Se creo Item de Item Hijo - Descuento de Agregacion:  " + sAdminC.getNombre() + " ,del padre: " + sAdminC.getNombreseqDescAgreg());
                                String mensaje = ("Se creo Item de Item Hijo - Descuento de Agregacion:  " + sAdminC.getNombre() + " ,del padre: " + sAdminC.getNombreseqDescAgreg());

                                String MsgSalida = "Creado Item de Item Hijo - Descuento de Agregacion";
                                this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                                Reportes rep = new Reportes();
                                rep.agregarTextoAlfinal(mensaje);

                                BCCHILD.release();
                                BC.release();
                                BO.release();
                            }

                        } else {
                            System.out.println("No se encontro registro de Seq. de Descuento de Agregacion con nombre: " + sAdminC.getNombreseqDescAgreg() + " , en el ambiente a insertar, por esta razon no puede validarse si existen HIJOS a modificar o crear. ");
                            String MsgSalida = "No se encontro registro de Seq. de Descuento de Agregacion con nombre: " + sAdminC.getNombreseqDescAgreg() + " , en el ambiente a insertar, por esta razon no puede validarse si existen HIJOS a modificar o crear. ";
                            String FlagCarga = "E";
                            String MsgError = "No se encontro registro de Seq. de Descuento de Agregacion con nombre: " + sAdminC.getNombreseqDescAgreg() + " , en el ambiente a insertar, por esta razon no puede validarse si existen HIJOS a modificar o crear. ";

                            Reportes rep = new Reportes();
                            rep.agregarTextoAlfinal(MsgError);
                            this.CargaBitacoraSalidaError(sAdminC.getRowid(), MsgSalida, FlagCarga, MsgError,usuario,version,ambienteInser,ambienteExtra);

                            BCCHILD.release();
                            BC.release();
                            BO.release();

                        }

                    } catch (SiebelException e) {
                        String error = e.getErrorMessage();
                        System.out.println("Error en creacion de Item Hijo - Descuento de Agregacion:  " + sAdminC.getNombre() + " ,del padre: " + sAdminC.getNombreseqDescAgreg() + " : " + "  , con el mensaje:   " + error.replace("'", " "));
                        String MsgSalida = "Error al Crear Item Hijo - Descuento de Agregacion";
                        String FlagCarga = "E";
                        String MsgError = "Error en creacion de Item Hijo - Descuento de Agregacion:  " + sAdminC.getNombre() + " ,del padre: " + sAdminC.getNombreseqDescAgreg() + " : " + "  , con el mensaje:   " + error.replace("'", " ");

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
            System.out.println("Error en  validacion Item Hijo - Descuento de Agregacion, con el error:  " + error.replace("'", " "));
            String MsgError = "Error en  validacion Item Hijo - Descuento de Agregacion, con el error:  " + error.replace("'", " ");
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(MsgError);

        }
    }

    @Override
    public void cargaBC(SiebelBusComp BC, SiebelBusComp oBCPick, String RowID, String IdPadre, String usuario,String version,String ambienteInser,String ambienteExtra) throws SiebelException {
        try {
            List<SeqDescAgregacionHijo> hijo = new LinkedList();
            hijo = this.consultaHijo(IdPadre,usuario,version,ambienteInser,ambienteExtra);
            for (SeqDescAgregacionHijo sAdminC : hijo) {
                try {
                    BC.activateField("Bundle Discount Name");
                    BC.activateField("Next Seq If True");
                    BC.activateField("Next Seq If False");
                    BC.activateField("Sequence");
                    BC.activateField("Bundle Sequence Id"); // Id Padre
                    BC.clearToQuery();
                    BC.setSearchExpr("[Bundle Discount Name]='" + sAdminC.getNombre() + "' AND [Sequence]= '" + sAdminC.getSecuencia() + "' AND [Next Seq If False] = '" + sAdminC.getSigDescuentoNoUtiliza() + "'");
                    BC.executeQuery(true);
                    boolean regchild = BC.firstRecord();
                    if (regchild) {
                        if (sAdminC.getSigDescuentoSiUtiliza() != null) {
                            if (!BC.getFieldValue("Next Seq If True").equals(sAdminC.getSigDescuentoSiUtiliza())) {
                                BC.setFieldValue("Next Seq If True", sAdminC.getSigDescuentoSiUtiliza());
                            }
                        }
//
//                        if (sAdminC.getSigDescuentoNoUtiliza() != null) {
//                            if (!BC.getFieldValue("Next Seq If False").equals(sAdminC.getSigDescuentoNoUtiliza())) {
//                                BC.setFieldValue("Next Seq If False", sAdminC.getSigDescuentoNoUtiliza());
//                            }
//                        }

                        BC.writeRecord();

                        System.out.println("Se valida existencia y/o actualizacion de Item Hijo - Descuento de Agregacion:  " + sAdminC.getNombre());
                        String MsgSalida = "Se valida existencia y/o actualizacion de Item Hijo - Descuento de Agregacion";
                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(MsgSalida);

                        this.CargaBitacoraSalidaValidado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                    } else {
                        BC.newRecord(0);

                        if (sAdminC.getSecuencia() != null) {
                            BC.setFieldValue("Sequence", sAdminC.getSecuencia());
                        }

                        if (sAdminC.getNombre() != null) {
                            oBCPick = BC.getPicklistBusComp("Bundle Discount Name");
                            oBCPick.clearToQuery();
                            oBCPick.setSearchExpr("[Name]='" + sAdminC.getNombre() + "' ");
                            oBCPick.executeQuery(true);
                            if (oBCPick.firstRecord()) {
                                oBCPick.pick();
                            }
                            oBCPick.release();
                        }

                        if (sAdminC.getSigDescuentoSiUtiliza() != null) {
                            BC.setFieldValue("Next Seq If True", sAdminC.getSigDescuentoSiUtiliza());
                        }

                        if (sAdminC.getSigDescuentoNoUtiliza() != null) {
                            BC.setFieldValue("Next Seq If False", sAdminC.getSigDescuentoNoUtiliza());
                        }

                        BC.setFieldValue("Bundle Sequence Id", RowID);

                        BC.writeRecord();

                        System.out.println("Se creo Item Hijo - Descuento de Agregacion:  " + sAdminC.getNombre());
                        String mensaje = ("Se creo Item Hijo - Descuento de Agregacion:  " + sAdminC.getNombre());

                        String MsgSalida = "Creado Item Hijo - Descuento de Agregacion";
                        this.CargaBitacoraSalidaCreado(sAdminC.getRowid(), MsgSalida, "Y", "",usuario,version,ambienteInser,ambienteExtra);

                        Reportes rep = new Reportes();
                        rep.agregarTextoAlfinal(mensaje);
                    }
                } catch (SiebelException e) {
                    String error = e.getErrorMessage();
                    System.out.println("Error en Hijo de Item Hijo - Descuento de Agregacion:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " "));
                    String MsgSalida = "Error en Hijo de Item Hijo - Descuento de Agregacion:  ";
                    String FlagCarga = "E";
                    String MsgError = "Error en Hijo de Item Hijo - Descuento de Agregacion:  " + sAdminC.getNombre() + "     , con el mensaje:   " + error.replace("'", " ");

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
            Logger.getLogger(DAOSeqDescAgregacionImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public List<SeqDescAgregacionPadre> consultaPadre(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT A.ROW_ID, A.BUNDLE_SEQ_NAME \"Nombre\", A.EFF_START_DT \"Vigente Desde\", A.EFF_END_DT \"Vigente Hasta\", A.ACTIVE_FLG \"Activo\", A.DESC_TEXT \"Comentarios\"\n"
                + "FROM SIEBEL.S_BUNDLE_SEQ A, SIEBEL.S_USER H\n"
                + "WHERE H.ROW_ID = A.LAST_UPD_BY\n"
                + "AND A.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = A.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY A.LAST_UPD ASC";
        List<SeqDescAgregacionPadre> seqDescAgregacion = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Padre a procesar.");

            while (rs.next()) {
                SeqDescAgregacionPadre lista = new SeqDescAgregacionPadre();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setVijenteDesde(rs.getDate("Vigente Desde"));
                lista.setVijenteHasta(rs.getDate("Vigente Hasta"));
                lista.setActivo(rs.getString("Activo"));
                lista.setComentario(rs.getString("Comentarios"));
                seqDescAgregacion.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"),rs.getString("Nombre"),"SE OBTIENEN DATOS","SEQ. DESCUENTO DE AGREGACION",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = seqDescAgregacion.size();
            Boolean conteo = seqDescAgregacion.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Padres Seq. Descuento de Agregacion o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Padres Seq. Descuento de Agregacion o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Padres Seq. Descuento de Agregacion, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Padres Seq. Descuento de Agregacion, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaPadre, con el error:  " + ex);
            throw ex;
        }
        return seqDescAgregacion;
    }

    @Override
    public List<SeqDescAgregacionHijo> consultaHijo(String IdPadre, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.BUNDLE_SEQ_ID \"Id Padre\", B.ITEM_SEQ_NUM \"Secuencia\", C.BUNDLE_DISCNT_NAME \"Nombre\", B.NEXT_Y_SEQ_NUM \"Sig descuen, si se utiliza\", \n"
                + "B.NEXT_N_SEQ_NUM \"Sig descuen, si no se utiliza\"\n"
                + "FROM SIEBEL.S_BDL_SEQ_ITEM B, SIEBEL.S_BUNDLE_DISCNT C, SIEBEL.S_USER H\n"
                + "WHERE B.BUNDLE_DISCNT_ID = C.ROW_ID AND B.LAST_UPD_BY = H.ROW_ID\n"
                + "AND B.BUNDLE_SEQ_ID = '" + IdPadre + "' AND H.LOGIN ='" + usuario + "'\n"
                + "AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA D WHERE D.ROW_ID = B.ROW_ID AND D.USUARIO ='" + usuario + "' AND D.VERSION ='" + version + "' AND D.EXTRAER ='" + ambienteExtra + "' AND D.INSERTAR ='" + ambienteInser + "')"
                + "ORDER BY B.CREATED ASC";
        List<SeqDescAgregacionHijo> seqDescAgregacion = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Hijos a procesar.");

            while (rs.next()) {
                SeqDescAgregacionHijo lista = new SeqDescAgregacionHijo();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setSigDescuentoSiUtiliza(rs.getString("Sig descuen, si se utiliza"));
                lista.setSigDescuentoNoUtiliza(rs.getString("Sig descuen, si no se utiliza"));
                lista.setSecuencia(rs.getString("Secuencia"));
                seqDescAgregacion.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre"), "SE OBTIENEN DATOS HIJO", "SEQ. DESCUENTO DE AGREGACION - HIJO",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = seqDescAgregacion.size();
            Boolean conteo = seqDescAgregacion.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Hijos - Seq. Descuento de Agregacion o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Hijos Seq. Descuento de Agregacion o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Hijos - Seq. Descuento de Agregacion, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Hijos Seq. Descuento de Agregacion, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo, con el error:  " + ex);
            throw ex;
        }
        return seqDescAgregacion;
    }

    @Override
    public List<SeqDescAgregacionSoloHijo> consultaSoloHijo(String fechaI, String fechaT,String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {
        String readRecordSQL = "SELECT B.ROW_ID, B.BUNDLE_SEQ_ID \"Id Padre\", B.ITEM_SEQ_NUM \"Secuencia\", C.BUNDLE_DISCNT_NAME \"Nombre\", B.NEXT_Y_SEQ_NUM \"Sig descuen, si se utiliza\",\n"
                + "B.NEXT_N_SEQ_NUM \"Sig descuen, si no se utiliza\", A.BUNDLE_SEQ_NAME \"Nombre SeqDesAgre\"\n"
                + "FROM SIEBEL.S_BDL_SEQ_ITEM B, SIEBEL.S_BUNDLE_DISCNT C, SIEBEL.S_BUNDLE_SEQ A,SIEBEL.S_USER H\n"
                + "WHERE B.BUNDLE_DISCNT_ID = C.ROW_ID AND B.BUNDLE_SEQ_ID = A.ROW_ID AND B.LAST_UPD_BY = H.ROW_ID\n"
                + "AND B.LAST_UPD BETWEEN TO_DATE ('" + fechaI + "', 'MM/dd/yyyy') AND TO_DATE ('" + fechaT + "', 'MM/dd/yyyy') +1\n"
                + "AND  H.LOGIN = '" + usuario + "' AND NOT EXISTS (SELECT ROW_ID FROM SIEBEL.CT_BITACORA E WHERE E.ROW_ID = B.ROW_ID AND E.USUARIO ='" + usuario + "' AND E.VERSION ='" + version + "' AND E.EXTRAER ='" + ambienteExtra + "' AND E.INSERTAR ='" + ambienteInser + "')\n"
                + "ORDER BY B.LAST_UPD ASC";
        List<SeqDescAgregacionSoloHijo> seqDescAgregacionsolohijo = new LinkedList();
        try (PreparedStatement ps = conexion.prepareStatement(readRecordSQL); ResultSet rs = ps.executeQuery()) {

            System.out.println("Creando Lista de registros Solo Hijos a procesar.");

            while (rs.next()) {
                SeqDescAgregacionSoloHijo lista = new SeqDescAgregacionSoloHijo();
                lista.setRowid(rs.getString("ROW_ID"));
                lista.setNombre(rs.getString("Nombre"));
                lista.setSigDescuentoSiUtiliza(rs.getString("Sig descuen, si se utiliza"));
                lista.setSigDescuentoNoUtiliza(rs.getString("Sig descuen, si no se utiliza"));
                lista.setSecuencia(rs.getString("Secuencia"));
                lista.setIdPadre(rs.getString("Id Padre"));
                lista.setNombreseqDescAgreg(rs.getString("Nombre SeqDesAgre"));
                seqDescAgregacionsolohijo.add(lista);
                this.CargaBitacoraEntrada(rs.getString("ROW_ID"), rs.getString("Nombre"), "SE OBTIENEN DATOS HIJO", "SEQ. DESCUENTO DE AGREGACION HIJO",usuario,version,ambienteInser,ambienteExtra);

            }
            int Registros = seqDescAgregacionsolohijo.size();
            Boolean conteo = seqDescAgregacionsolohijo.isEmpty(); // Valida si la lista esta vacia
            String mensaje = "";
            if (conteo) {
                System.out.println("No se obtuvieron listas de Solo Hijos - Seq. Descuento de Agregacion o estas ya fueron procesadas.");
                mensaje = "No se obtuvieron listas de Solo Hijos Seq. Descuento de Agregacion o estas ya fueron procesadas.";
            } else {
                System.out.println("Se obtiene listas de Solo Hijos - Seq. Descuento de Agregacion, se procesaran: " + Registros + " registros.");
                mensaje = "Se obtiene listas de Solo Hijos Seq. Descuento de Agregacion, se procesaran: " + Registros + " registros.";
            }
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(mensaje);

            rs.close();
            ps.close();

        } catch (SQLException ex) {
            System.out.println("Error en ConsultaHijo, con el error:  " + ex);
            throw ex;
        }
        return seqDescAgregacionsolohijo;
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
    }

    private void CargaBitacoraEntrada(String Row_Id, String Val1, String Seguimiento, String Objeto, String usuario,String version,String ambienteInser,String ambienteExtra) throws Exception {

//        String TipoObjeto = Val1 + " : " + Val2 + " : " + Val3;
        String searchRecordSQL1 = "SELECT ROW_ID FROM SIEBEL.CT_BITACORA WHERE ROW_ID = '" + Row_Id + "' AND USUARIO = '" + usuario + "' AND VERSION = '" + version + "' AND EXTRAER = '" + ambienteExtra + "' AND INSERTAR = '" + ambienteInser + "'";

        try (PreparedStatement ps = conexion.prepareStatement(searchRecordSQL1); ResultSet rs1 = ps.executeQuery()) {
            if (rs1.next()) {
                // sin accion
            } else {
                String setRecordSQL2 = "INSERT INTO SIEBEL.CT_BITACORA  (ROW_ID, TIPO_CATALOGO, TIPO_OBJETO, FLAG_CARGA, SEGUIMIENTO, MENSAJE_ERROR,USUARIO, VERSION, EXTRAER, INSERTAR)\n"
                        + "VALUES('" + Row_Id + "','" + Objeto + "','" + Val1 + "','N','" + Seguimiento + "','','" + usuario + "','" + version + "','" + ambienteExtra + "','" + ambienteInser + "')";

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
