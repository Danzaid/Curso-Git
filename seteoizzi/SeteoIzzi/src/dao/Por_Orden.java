package dao;

import izziImpl.DAOAccionesPolizasImpl;
import izziImpl.DAOActividadCasoNegocioImpl;
import izziImpl.DAOAdministracionListaPreciosClienteImpl;
import izziImpl.DAOAdministracionMatrizDescuentoImpl;
import izziImpl.DAOAdministracionRazonesSocialesImpl;
import izziImpl.DAOAdministracionTransaccionesInventarioImpl;
import izziImpl.DAOAltaPromocionesImpl;
import izziImpl.DAOAsuntoMensajesImpl;
import izziImpl.DAOCatalogoProductoPlazoForzosoImpl;
import izziImpl.DAOCodigoBarrasFSImpl;
import izziImpl.DAOConjuntoReglasValidacionImpl;
import izziImpl.DAOConsultasPredefinidasImpl;
import izziImpl.DAOControladorCasosNegocioImpl;
import izziImpl.DAOControladorOfertaEquipoImpl;
import izziImpl.DAOControladorRTPImpl;
import izziImpl.DAODescuentoAgregacionImpl;
import izziImpl.DAOEventosTiempoEjecucionImpl;
import izziImpl.DAOFallasGeneralesImpl;
import izziImpl.DAOGrupoPolizasImpl;
import izziImpl.DAOIndustriasImpl;
import izziImpl.DAOInterciudadesImpl;
import izziImpl.DAOListaCompañiasImpl;
import izziImpl.DAOListaPolizasImpl;
import izziImpl.DAOListaPreciosImpl;
import izziImpl.DAOListaValoresImpl;
import izziImpl.DAOMatricesElegibilidadCompImpl;
import izziImpl.DAOMensajesValidacionImpl;
import izziImpl.DAOModelosEstadostransPuestoTrabajoImpl;
import izziImpl.DAOOficinasOtrosMediosPagoImpl;
import izziImpl.DAOPuntosInventarioImpl;
import izziImpl.DAOSeqDescAgregacionImpl;
import java.io.File;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JOptionPane;

public class Por_Orden extends javax.swing.JFrame {

    private String urlEx;
    private String passEx;
    private String usuarioEx;
    private String urlIn;
    private String passIn;
    private String usuarioIn;

    public Por_Orden() {
        initComponents();
    }

    public Por_Orden(String userEx, String passEx, String urlEx, String userIn, String passIn, String urlIn) {
        initComponents();
        this.urlEx = urlEx;
        this.passEx = passEx;
        this.usuarioEx = userEx;
        this.urlIn = urlIn;
        this.passIn = passIn;
        this.usuarioIn = userIn;

    }

    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        Forma = new javax.swing.ButtonGroup();
        jPanel2 = new javax.swing.JPanel();
        jScrollPane1 = new javax.swing.JScrollPane();
        jPanel1 = new javax.swing.JPanel();
        jLabel3 = new javax.swing.JLabel();
        jLabel4 = new javax.swing.JLabel();
        jLabel6 = new javax.swing.JLabel();
        jLabel7 = new javax.swing.JLabel();
        jLabel8 = new javax.swing.JLabel();
        jLabel9 = new javax.swing.JLabel();
        jLabel10 = new javax.swing.JLabel();
        jLabel11 = new javax.swing.JLabel();
        jLabel12 = new javax.swing.JLabel();
        jLabel13 = new javax.swing.JLabel();
        jLabel14 = new javax.swing.JLabel();
        jLabel15 = new javax.swing.JLabel();
        jLabel16 = new javax.swing.JLabel();
        jLabel17 = new javax.swing.JLabel();
        jLabel18 = new javax.swing.JLabel();
        jLabel19 = new javax.swing.JLabel();
        jLabel20 = new javax.swing.JLabel();
        jLabel21 = new javax.swing.JLabel();
        jLabel22 = new javax.swing.JLabel();
        jLabel23 = new javax.swing.JLabel();
        jLabel24 = new javax.swing.JLabel();
        jLabel25 = new javax.swing.JLabel();
        jLabel26 = new javax.swing.JLabel();
        jLabel27 = new javax.swing.JLabel();
        jLabel28 = new javax.swing.JLabel();
        jLabel29 = new javax.swing.JLabel();
        jLabel30 = new javax.swing.JLabel();
        jLabel31 = new javax.swing.JLabel();
        jLabel32 = new javax.swing.JLabel();
        jLabel33 = new javax.swing.JLabel();
        jLabel35 = new javax.swing.JLabel();
        jButton1 = new javax.swing.JButton();
        jButton2 = new javax.swing.JButton();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
        setTitle("Forma de Ejecucion");

        jPanel2.setBorder(javax.swing.BorderFactory.createTitledBorder(javax.swing.BorderFactory.createEtchedBorder(), "Orden de aplicacion"));

        jPanel1.setBorder(javax.swing.BorderFactory.createTitledBorder(javax.swing.BorderFactory.createEtchedBorder()));

        jLabel3.setText("27.- Acciones de Pólizas");
        jLabel3.setToolTipText("");

        jLabel4.setText("18.-Administración de Matriz de Descuentos");
        jLabel4.setToolTipText("");

        jLabel6.setText("8.-Administración de Razones Sociales");

        jLabel7.setText("11.-Administración de Transacciones de Inventario");
        jLabel7.setToolTipText("");

        jLabel8.setText("22.-Alta Promociones");
        jLabel8.setToolTipText("");

        jLabel9.setText("5.-Asunto Mensajes");

        jLabel10.setText("21.-Catalogo de Producto y Plazo Forzoso");

        jLabel11.setText("14.-Conjunto de reglas de validación, Reglas y Argumentos de las reglas");

        jLabel12.setText("2.-Consultas Predefinidas");

        jLabel13.setText("24.-Controlador de Casos de Negocio");

        jLabel14.setText("23.-Controlador de Oferta - Equipo");

        jLabel15.setText("4.-Controlador RPT");

        jLabel16.setText("13.-Eventos de tiempo de ejecución");

        jLabel17.setText("26.- Fallas Generales");
        jLabel17.setToolTipText("");

        jLabel18.setText("28.- Grupo de Pólizas");

        jLabel19.setText("7.-Industrias");

        jLabel20.setText("6.-Interciudades");

        jLabel21.setText("9.-Listas de Compañias");

        jLabel22.setText("30.- Lista de Precios");

        jLabel23.setText("31.-Lista de Precios por cliente");

        jLabel24.setText("29.-Listas de Pólizas (Pólizas de flujo de trabajo)");

        jLabel25.setText("1.- Lista de Valores");

        jLabel26.setText("15.-Mapeos de Códigos de Barras de FS");

        jLabel27.setText("17.-Matrices de Elegibilidad y Compatibilidad");

        jLabel28.setText("16.-Mensajes de validación");
        jLabel28.setToolTipText("");

        jLabel29.setText("3.- Modelos de Estado, Transacciones y Puestos de trabajo");

        jLabel30.setText("12.-Oficinas – Otros Medios de Pago");

        jLabel31.setText("25.- Plantillas de Actividad Casos de Negocio (CN)");

        jLabel32.setText("20.-Secuencia de Descuentos de Agregación");

        jLabel33.setText("10.-Todos los Puntos de Inventario");
        jLabel33.setToolTipText("");

        jLabel35.setText("19.-Descuentos de Agregación");

        javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
        jPanel1.setLayout(jPanel1Layout);
        jPanel1Layout.setHorizontalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addGap(35, 35, 35)
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(jPanel1Layout.createSequentialGroup()
                        .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(jLabel26)
                            .addComponent(jLabel11))
                        .addGap(0, 0, Short.MAX_VALUE))
                    .addGroup(jPanel1Layout.createSequentialGroup()
                        .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                            .addComponent(jLabel28)
                            .addComponent(jLabel16)
                            .addComponent(jLabel6)
                            .addComponent(jLabel9)
                            .addComponent(jLabel33)
                            .addComponent(jLabel21)
                            .addComponent(jLabel30)
                            .addComponent(jLabel20)
                            .addComponent(jLabel15)
                            .addComponent(jLabel7)
                            .addComponent(jLabel19)
                            .addComponent(jLabel29)
                            .addComponent(jLabel12)
                            .addComponent(jLabel25)
                            .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                                .addComponent(jLabel27)
                                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                    .addComponent(jLabel35)
                                    .addComponent(jLabel4)))
                            .addGroup(jPanel1Layout.createSequentialGroup()
                                .addGap(1, 1, 1)
                                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                    .addComponent(jLabel10)
                                    .addComponent(jLabel32)
                                    .addComponent(jLabel8)
                                    .addComponent(jLabel14)
                                    .addComponent(jLabel13)
                                    .addComponent(jLabel31)
                                    .addComponent(jLabel17)
                                    .addComponent(jLabel3)
                                    .addComponent(jLabel18)
                                    .addComponent(jLabel24)
                                    .addComponent(jLabel22)
                                    .addComponent(jLabel23))))
                        .addContainerGap(223, Short.MAX_VALUE))))
        );
        jPanel1Layout.setVerticalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jLabel25)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel12)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel29)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel15)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel9)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel20)
                .addGap(8, 8, 8)
                .addComponent(jLabel19)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel6)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel21)
                .addGap(6, 6, 6)
                .addComponent(jLabel33)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel7)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel30)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel16)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel11)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel26)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel28)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel27)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel4)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel35)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel32)
                .addGap(6, 6, 6)
                .addComponent(jLabel10, javax.swing.GroupLayout.PREFERRED_SIZE, 12, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel8)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel14)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel13)
                .addGap(6, 6, 6)
                .addComponent(jLabel31)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel17)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel3)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel18)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel24)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel22)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jLabel23)
                .addContainerGap(50, Short.MAX_VALUE))
        );

        jScrollPane1.setViewportView(jPanel1);

        javax.swing.GroupLayout jPanel2Layout = new javax.swing.GroupLayout(jPanel2);
        jPanel2.setLayout(jPanel2Layout);
        jPanel2Layout.setHorizontalGroup(
            jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(jScrollPane1, javax.swing.GroupLayout.DEFAULT_SIZE, 566, Short.MAX_VALUE)
        );
        jPanel2Layout.setVerticalGroup(
            jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel2Layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jScrollPane1, javax.swing.GroupLayout.DEFAULT_SIZE, 535, Short.MAX_VALUE))
        );

        jButton1.setText("Inicio");
        jButton1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton1ActionPerformed(evt);
            }
        });

        jButton2.setText("Regresar");
        jButton2.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton2ActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jPanel2, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addContainerGap())
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                .addGap(112, 112, 112)
                .addComponent(jButton1, javax.swing.GroupLayout.PREFERRED_SIZE, 124, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(61, 61, 61)
                .addComponent(jButton2, javax.swing.GroupLayout.PREFERRED_SIZE, 114, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addGap(20, 20, 20)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jButton1, javax.swing.GroupLayout.PREFERRED_SIZE, 36, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jButton2, javax.swing.GroupLayout.PREFERRED_SIZE, 36, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                .addComponent(jPanel2, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addContainerGap())
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents


    private void jButton1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton1ActionPerformed

        String usuario = Ejecuta_Todo.jTextField2.getText();
        String version = Ejecuta_Todo.jTextField3.getText();
        boolean in = true;
        Ejecuta_Todo.jTextField1.getText();
        Fechas fe = new Fechas();
        String fechaIni = Ejecuta_Todo.fecha1;
        String fechaTer = Ejecuta_Todo.fecha2;
        String iteraciones = Ejecuta_Todo.jTextField1.getText();
        String ambienteInser = Ejecuta_Todo.AmbienteI;
        String ambienteExtra = Ejecuta_Todo.AmbienteE;
        
        
       String usuEsq = Ejecuta_Todo.jTextField4.getText();
        
//        if (in) {
//            DAOListaValoresImpl imp = new DAOListaValoresImpl();
//            String titulo = "implementacion DAO Lista de Valores";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//
//        if (in) {
//            DAOConsultasPredefinidasImpl imp = new DAOConsultasPredefinidasImpl();
//            String titulo = "implementacion DAO Consultas Predefinidas";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOModelosEstadostransPuestoTrabajoImpl imp = new DAOModelosEstadostransPuestoTrabajoImpl();
//            String titulo = "implementacion DAO Modelos de Estado, Transacciones y Puestos de trabajo";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOControladorRTPImpl imp = new DAOControladorRTPImpl();
//            String titulo = "implementacion DAO Controlador RPT";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOAsuntoMensajesImpl imp = new DAOAsuntoMensajesImpl();
//            String titulo = "implementacion DAO Asunto Mensajes";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOInterciudadesImpl imp = new DAOInterciudadesImpl();
//            String titulo = "implementacion DAO Interciudades";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOIndustriasImpl imp = new DAOIndustriasImpl();
//            String titulo = "implementacion DAO Industrias";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOAdministracionRazonesSocialesImpl imp = new DAOAdministracionRazonesSocialesImpl();
//            String titulo = "implementacion DAO Administración de Razones Sociales";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOListaCompañiasImpl imp = new DAOListaCompañiasImpl();
//            String titulo = "implementacion DAO Lista de Compañias";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOPuntosInventarioImpl imp = new DAOPuntosInventarioImpl();
//            String titulo = "implementacion DAO Todos los Puntos de Inventario";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//        }
//        if (in) {
//            DAOAdministracionTransaccionesInventarioImpl imp = new DAOAdministracionTransaccionesInventarioImpl();
//            String titulo = "implementacion DAO Administración de Transacciones de Inventario";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOOficinasOtrosMediosPagoImpl imp = new DAOOficinasOtrosMediosPagoImpl();
//            String titulo = "implementacion DAO Oficinas – Otros Medios de Pago";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOEventosTiempoEjecucionImpl imp = new DAOEventosTiempoEjecucionImpl();
//            String titulo = "implementacion DAO Eventos de tiempo de ejecución";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOConjuntoReglasValidacionImpl imp = new DAOConjuntoReglasValidacionImpl();
//            String titulo = "implementacion DAO Conjunto de reglas de validación, Reglas y Argumentos de las reglas";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOCodigoBarrasFSImpl imp = new DAOCodigoBarrasFSImpl();
//            String titulo = "implementacion DAO Mapeos de Códigos de Barras de FS";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOMensajesValidacionImpl imp = new DAOMensajesValidacionImpl();
//            String titulo = "implementacion DAO Mensajes de validación";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOMatricesElegibilidadCompImpl imp = new DAOMatricesElegibilidadCompImpl();
//            String titulo = "implementacion DAO Matrices de Elegibilidad y Compatibilidad";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//
//            DAOAdministracionMatrizDescuentoImpl imp = new DAOAdministracionMatrizDescuentoImpl();
//            String titulo = "implementacion DAO Administracion De Matriz de Descuentos";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAODescuentoAgregacionImpl imp = new DAODescuentoAgregacionImpl();
//            String titulo = "implementacion DAO Descuentos de Agregacion";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOSeqDescAgregacionImpl imp = new DAOSeqDescAgregacionImpl();
//            String titulo = "implementacion DAO Secuencia de Descuentos de Agregación";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOCatalogoProductoPlazoForzosoImpl imp = new DAOCatalogoProductoPlazoForzosoImpl();
//            String titulo = "implementacion DAO Catalogo de Producto y Plazo Forzoso";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOAltaPromocionesImpl imp = new DAOAltaPromocionesImpl();
//            String titulo = "implementacion DAO Alta Promociones";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOControladorOfertaEquipoImpl imp = new DAOControladorOfertaEquipoImpl();
//            String titulo = "implementacion DAO Controlador de Oferta - Equipo";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOControladorCasosNegocioImpl imp = new DAOControladorCasosNegocioImpl();
//            String titulo = "implementacion DAO Controlador de Casos de Negocio";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOActividadCasoNegocioImpl imp = new DAOActividadCasoNegocioImpl();
//            String titulo = "implementacion DAO Plantillas de Actividad Casos de Negocio (CN)";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOFallasGeneralesImpl imp = new DAOFallasGeneralesImpl();
//            String titulo = "implementacion DAO Fallas Generales";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOAccionesPolizasImpl imp = new DAOAccionesPolizasImpl();
//            String titulo = "implementacion DAO Acciones de Pólizas ";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOGrupoPolizasImpl imp = new DAOGrupoPolizasImpl();
//            String titulo = "implementacion DAO Grupo de Pólizas";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOListaPolizasImpl imp = new DAOListaPolizasImpl();
//            String titulo = "implementacion DAO Listas de Pólizas (Pólizas de flujo de trabajo)";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//
//            DAOAdministracionListaPreciosClienteImpl imp = new DAOAdministracionListaPreciosClienteImpl();
//            String titulo = "implementacion DAO Lista de Precios por cliente.";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
//        if (in) {
//            DAOListaPreciosImpl imp = new DAOListaPreciosImpl();
//            String titulo = "implementacion DAO Lista de Precios";
//            Reportes rep = new Reportes();
//            rep.agregarTextoAlfinal(titulo);
//            imp.getHora();
//
//            try {
//                imp.ConectarDB(urlEx, usuarioEx, passEx);
//
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
//            } catch (Exception ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//                String mensaje = "el error es:" + ex.getMessage();
//                rep.agregarTextoAlfinal(mensaje);
//            }
//            try {
//                imp.CloseDB();
//            } catch (SQLException ex) {
//                Logger.getLogger(Ejecuta_Todo.class.getName()).log(Level.SEVERE, null, ex);
//            }
//            System.out.println("Termino correcto implementacion DAO");
//            String mensaje = "Termino correcto implementacion DAO";
//
//            rep.agregarTextoAlfinal(mensaje);
//            imp.getHora();
//            String linea = "--------------------------------------------------------------------------";
//            rep.agregarTextoAlfinal(linea);
//
//        }
        File ruta = new File("");
        JOptionPane.showMessageDialog(this, "el reporte se creo en le sigiente ruta: " + ruta.getAbsolutePath() + "\\ReporteSeteo.txt");


    }//GEN-LAST:event_jButton1ActionPerformed

    private void jButton2ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton2ActionPerformed
        Ejecuta_Todo prin = new Ejecuta_Todo();
        prin.setVisible(true);
        this.setVisible(false);
    }//GEN-LAST:event_jButton2ActionPerformed

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {

        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(Por_Orden.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(Por_Orden.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(Por_Orden.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(Por_Orden.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new Por_Orden().setVisible(true);
            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.ButtonGroup Forma;
    private javax.swing.JButton jButton1;
    private javax.swing.JButton jButton2;
    private javax.swing.JLabel jLabel10;
    private javax.swing.JLabel jLabel11;
    private javax.swing.JLabel jLabel12;
    private javax.swing.JLabel jLabel13;
    private javax.swing.JLabel jLabel14;
    private javax.swing.JLabel jLabel15;
    private javax.swing.JLabel jLabel16;
    private javax.swing.JLabel jLabel17;
    private javax.swing.JLabel jLabel18;
    private javax.swing.JLabel jLabel19;
    private javax.swing.JLabel jLabel20;
    private javax.swing.JLabel jLabel21;
    private javax.swing.JLabel jLabel22;
    private javax.swing.JLabel jLabel23;
    private javax.swing.JLabel jLabel24;
    private javax.swing.JLabel jLabel25;
    private javax.swing.JLabel jLabel26;
    private javax.swing.JLabel jLabel27;
    private javax.swing.JLabel jLabel28;
    private javax.swing.JLabel jLabel29;
    private javax.swing.JLabel jLabel3;
    private javax.swing.JLabel jLabel30;
    private javax.swing.JLabel jLabel31;
    private javax.swing.JLabel jLabel32;
    private javax.swing.JLabel jLabel33;
    private javax.swing.JLabel jLabel35;
    private javax.swing.JLabel jLabel4;
    private javax.swing.JLabel jLabel6;
    private javax.swing.JLabel jLabel7;
    private javax.swing.JLabel jLabel8;
    private javax.swing.JLabel jLabel9;
    private javax.swing.JPanel jPanel1;
    private javax.swing.JPanel jPanel2;
    private javax.swing.JScrollPane jScrollPane1;
    // End of variables declaration//GEN-END:variables
}
