/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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


/**
 *
 * @author hector.pineda
 */
public class Por_Documento extends javax.swing.JFrame {

    private String urlEx;
    private String passEx;
    private String usuarioEx;
    private String urlIn;
    private String passIn;
    private String usuarioIn;

    /**
     * Creates new form Principal
     */
    public Por_Documento() {
        initComponents();
    }

    public Por_Documento(String userEx, String passEx, String urlEx, String userIn, String passIn, String urlIn) {
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

        buttonGroup1 = new javax.swing.ButtonGroup();
        jButton2 = new javax.swing.JButton();
        jButton3 = new javax.swing.JButton();
        jPanel2 = new javax.swing.JPanel();
        jScrollPane1 = new javax.swing.JScrollPane();
        jPanel5 = new javax.swing.JPanel();
        jRadioButton1 = new javax.swing.JRadioButton();
        jRadioButton2 = new javax.swing.JRadioButton();
        jRadioButton3 = new javax.swing.JRadioButton();
        jRadioButton4 = new javax.swing.JRadioButton();
        jRadioButton6 = new javax.swing.JRadioButton();
        jRadioButton7 = new javax.swing.JRadioButton();
        jRadioButton8 = new javax.swing.JRadioButton();
        jRadioButton9 = new javax.swing.JRadioButton();
        jRadioButton10 = new javax.swing.JRadioButton();
        jRadioButton11 = new javax.swing.JRadioButton();
        jRadioButton12 = new javax.swing.JRadioButton();
        jRadioButton13 = new javax.swing.JRadioButton();
        jRadioButton14 = new javax.swing.JRadioButton();
        jRadioButton15 = new javax.swing.JRadioButton();
        jRadioButton16 = new javax.swing.JRadioButton();
        jRadioButton18 = new javax.swing.JRadioButton();
        jRadioButton19 = new javax.swing.JRadioButton();
        jRadioButton20 = new javax.swing.JRadioButton();
        jRadioButton21 = new javax.swing.JRadioButton();
        jRadioButton22 = new javax.swing.JRadioButton();
        jRadioButton23 = new javax.swing.JRadioButton();
        jRadioButton24 = new javax.swing.JRadioButton();
        jRadioButton25 = new javax.swing.JRadioButton();
        jRadioButton26 = new javax.swing.JRadioButton();
        jRadioButton28 = new javax.swing.JRadioButton();
        jRadioButton29 = new javax.swing.JRadioButton();
        jRadioButton30 = new javax.swing.JRadioButton();
        jRadioButton31 = new javax.swing.JRadioButton();
        jRadioButton32 = new javax.swing.JRadioButton();
        jRadioButton33 = new javax.swing.JRadioButton();
        jRadioButton34 = new javax.swing.JRadioButton();
        jRadioButton17 = new javax.swing.JRadioButton();
        jButton1 = new javax.swing.JButton();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

        jButton2.setText("Iniciar");
        jButton2.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton2ActionPerformed(evt);
            }
        });

        jButton3.setText("Regresar");
        jButton3.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton3ActionPerformed(evt);
            }
        });

        jPanel2.setBorder(javax.swing.BorderFactory.createTitledBorder(javax.swing.BorderFactory.createEtchedBorder(), "seleccione las tablas"));

        buttonGroup1.add(jRadioButton1);
        jRadioButton1.setText("Acciones de Pólizas");
        jRadioButton1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jRadioButton1ActionPerformed(evt);
            }
        });

        buttonGroup1.add(jRadioButton2);
        jRadioButton2.setText("Plantillas de Actividad Casos de Negocio (CN)");
        jRadioButton2.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jRadioButton2ActionPerformed(evt);
            }
        });

        buttonGroup1.add(jRadioButton3);
        jRadioButton3.setText("Lista de Precios por cliente");

        buttonGroup1.add(jRadioButton4);
        jRadioButton4.setText("Administración de Matriz de Descuentos");

        buttonGroup1.add(jRadioButton6);
        jRadioButton6.setText("Administración de Razones Sociales");

        buttonGroup1.add(jRadioButton7);
        jRadioButton7.setText("Administración de Transacciones de Inventario");

        buttonGroup1.add(jRadioButton8);
        jRadioButton8.setText("Alta Promociones");

        buttonGroup1.add(jRadioButton9);
        jRadioButton9.setText("Asunto Mensajes");

        buttonGroup1.add(jRadioButton10);
        jRadioButton10.setText("Catalogo de Producto y Plazo Forzoso");

        buttonGroup1.add(jRadioButton11);
        jRadioButton11.setText("Mapeos de Códigos de Barras de FS");

        buttonGroup1.add(jRadioButton12);
        jRadioButton12.setText("Conjunto de reglas de validación, Reglas y Argumentos de las reglas");
        jRadioButton12.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jRadioButton12ActionPerformed(evt);
            }
        });

        buttonGroup1.add(jRadioButton13);
        jRadioButton13.setText("Consultas Predefinidas");
        jRadioButton13.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jRadioButton13ActionPerformed(evt);
            }
        });

        buttonGroup1.add(jRadioButton14);
        jRadioButton14.setText("Controlador de Casos de Negocio");

        buttonGroup1.add(jRadioButton15);
        jRadioButton15.setText("Controlador de Oferta - Equipo");

        buttonGroup1.add(jRadioButton16);
        jRadioButton16.setText("Controlador RPT");

        buttonGroup1.add(jRadioButton18);
        jRadioButton18.setText("Secuencia de Descuentos de Agregación");

        buttonGroup1.add(jRadioButton19);
        jRadioButton19.setText("Eventos de tiempo de ejecución");

        buttonGroup1.add(jRadioButton20);
        jRadioButton20.setText("Fallas Generales");

        buttonGroup1.add(jRadioButton21);
        jRadioButton21.setText("Grupo de Pólizas");

        buttonGroup1.add(jRadioButton22);
        jRadioButton22.setText("Industrias");

        buttonGroup1.add(jRadioButton23);
        jRadioButton23.setText("Interciudades");
        jRadioButton23.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jRadioButton23ActionPerformed(evt);
            }
        });

        buttonGroup1.add(jRadioButton24);
        jRadioButton24.setText("Listas de Compañias");

        buttonGroup1.add(jRadioButton25);
        jRadioButton25.setText("Listas de Pólizas (Pólizas de flujo de trabajo)");

        buttonGroup1.add(jRadioButton26);
        jRadioButton26.setText("Lista de Precios");

        buttonGroup1.add(jRadioButton28);
        jRadioButton28.setText("Lista de Valores");

        buttonGroup1.add(jRadioButton29);
        jRadioButton29.setText("Matrices de Elegibilidad y Compatibilidad");

        buttonGroup1.add(jRadioButton30);
        jRadioButton30.setText("Mensajes de validación");

        buttonGroup1.add(jRadioButton31);
        jRadioButton31.setText("Modelos de Estado, Transacciones y Puestos de trabajo");

        buttonGroup1.add(jRadioButton32);
        jRadioButton32.setText("Oficinas – Otros Medios de Pago");

        buttonGroup1.add(jRadioButton33);
        jRadioButton33.setText("Catalogo de Producto y Plazo Forzoso");

        buttonGroup1.add(jRadioButton34);
        jRadioButton34.setText("Todos los Puntos de Inventario");

        buttonGroup1.add(jRadioButton17);
        jRadioButton17.setText("Descuentos de Agregacion");

        javax.swing.GroupLayout jPanel5Layout = new javax.swing.GroupLayout(jPanel5);
        jPanel5.setLayout(jPanel5Layout);
        jPanel5Layout.setHorizontalGroup(
            jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel5Layout.createSequentialGroup()
                .addGroup(jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jRadioButton12)
                    .addComponent(jRadioButton2)
                    .addComponent(jRadioButton11)
                    .addComponent(jRadioButton28)
                    .addComponent(jRadioButton25)
                    .addComponent(jRadioButton7)
                    .addComponent(jRadioButton18)
                    .addComponent(jRadioButton34))
                .addGap(0, 0, Short.MAX_VALUE))
            .addGroup(jPanel5Layout.createSequentialGroup()
                .addGroup(jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jRadioButton31)
                    .addComponent(jRadioButton23)
                    .addComponent(jRadioButton30)
                    .addComponent(jRadioButton10)
                    .addComponent(jRadioButton8)
                    .addComponent(jRadioButton9)
                    .addComponent(jRadioButton3)
                    .addComponent(jRadioButton26)
                    .addComponent(jRadioButton29)
                    .addComponent(jRadioButton32)
                    .addComponent(jRadioButton6)
                    .addComponent(jRadioButton4)
                    .addComponent(jRadioButton1)
                    .addComponent(jRadioButton14)
                    .addComponent(jRadioButton13)
                    .addComponent(jRadioButton16)
                    .addComponent(jRadioButton15)
                    .addComponent(jRadioButton22)
                    .addComponent(jRadioButton21)
                    .addComponent(jRadioButton24)
                    .addComponent(jRadioButton17)
                    .addComponent(jRadioButton33)
                    .addComponent(jRadioButton19)
                    .addComponent(jRadioButton20))
                .addContainerGap(198, Short.MAX_VALUE))
        );
        jPanel5Layout.setVerticalGroup(
            jPanel5Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel5Layout.createSequentialGroup()
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addComponent(jRadioButton1)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton4)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton6)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton7)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton8)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton9)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton10)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton12)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton13)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton14)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton15)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton16)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton33)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton17)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton19)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton20)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton21)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton22)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton23)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton24)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton26)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton3)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton25)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton28)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton11)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton29)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton30)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton31)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton32)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton2)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton18)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jRadioButton34)
                .addGap(8, 8, 8))
        );

        jScrollPane1.setViewportView(jPanel5);

        javax.swing.GroupLayout jPanel2Layout = new javax.swing.GroupLayout(jPanel2);
        jPanel2.setLayout(jPanel2Layout);
        jPanel2Layout.setHorizontalGroup(
            jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(jScrollPane1, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, 444, Short.MAX_VALUE)
        );
        jPanel2Layout.setVerticalGroup(
            jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, jPanel2Layout.createSequentialGroup()
                .addGap(0, 0, Short.MAX_VALUE)
                .addComponent(jScrollPane1, javax.swing.GroupLayout.PREFERRED_SIZE, 297, javax.swing.GroupLayout.PREFERRED_SIZE))
        );

        jButton1.setText("Salir");
        jButton1.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                jButton1ActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addGap(22, 22, 22)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addComponent(jPanel2, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addGroup(layout.createSequentialGroup()
                        .addComponent(jButton2, javax.swing.GroupLayout.PREFERRED_SIZE, 123, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                        .addComponent(jButton3, javax.swing.GroupLayout.PREFERRED_SIZE, 128, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGap(37, 37, 37)
                        .addComponent(jButton1, javax.swing.GroupLayout.PREFERRED_SIZE, 123, javax.swing.GroupLayout.PREFERRED_SIZE)))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addGap(19, 19, 19)
                .addGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jButton1)
                    .addComponent(jButton2)
                    .addComponent(jButton3))
                .addGap(18, 18, 18)
                .addComponent(jPanel2, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addContainerGap(24, Short.MAX_VALUE))
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents


    private void jButton2ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton2ActionPerformed

        Ejecuta_Todo.jTextField1.getText();
        Fechas fe = new Fechas();
        String fechaIni = Ejecuta_Todo.fecha1;
        String fechaTer = Ejecuta_Todo.fecha2;
        String usuario = Ejecuta_Todo.usuario;
        String version = Ejecuta_Todo.version;
         String usuEsq = Ejecuta_Todo.usuEsq;
        String ambienteInser = Ejecuta_Todo.AmbienteI;
        String ambienteExtra = Ejecuta_Todo.AmbienteE;
       

        String iteraciones = Ejecuta_Todo.ite;
        

        if (jRadioButton1.isSelected()) {
            DAOAccionesPolizasImpl imp = new DAOAccionesPolizasImpl();
            String titulo = "implementacion DAO Acciones de Pólizas ";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";
            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }

//        if (jRadioButton2.isSelected()) {
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
//        }
        if (jRadioButton3.isSelected()) {
            DAOAdministracionListaPreciosClienteImpl imp = new DAOAdministracionListaPreciosClienteImpl();
            String titulo = "implementacion DAO Lista de Precios por cliente.";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton4.isSelected()) {
            DAOAdministracionMatrizDescuentoImpl imp = new DAOAdministracionMatrizDescuentoImpl();
            String titulo = "implementacion DAO Administracion De Matriz de Descuentos";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }

        if (jRadioButton6.isSelected()) {
            DAOAdministracionRazonesSocialesImpl imp = new DAOAdministracionRazonesSocialesImpl();
            String titulo = "implementacion DAO Administración de Razones Sociales";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton7.isSelected()) {
            DAOAdministracionTransaccionesInventarioImpl imp = new DAOAdministracionTransaccionesInventarioImpl();
            String titulo = "implementacion DAO Administración de Transacciones de Inventario";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton8.isSelected()) {
            DAOAltaPromocionesImpl imp = new DAOAltaPromocionesImpl();
            String titulo = "implementacion DAO Alta Promociones";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton9.isSelected()) {
            DAOAsuntoMensajesImpl imp = new DAOAsuntoMensajesImpl();
            String titulo = "implementacion DAO Asunto Mensajes";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton10.isSelected()) {
            DAOCatalogoProductoPlazoForzosoImpl imp = new DAOCatalogoProductoPlazoForzosoImpl();
            String titulo = "implementacion DAO Catalogo de Producto y Plazo Forzoso";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton11.isSelected()) {
            DAOCodigoBarrasFSImpl imp = new DAOCodigoBarrasFSImpl();
            String titulo = "implementacion DAO Mapeos de Códigos de Barras de FS";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton12.isSelected()) {
            DAOConjuntoReglasValidacionImpl imp = new DAOConjuntoReglasValidacionImpl();
            String titulo = "implementacion DAO Conjunto de reglas de validación, Reglas y Argumentos de las reglas";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton13.isSelected()) {
            DAOConsultasPredefinidasImpl imp = new DAOConsultasPredefinidasImpl();
            String titulo = "implementacion DAO Consultas Predefinidas";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton14.isSelected()) {
            DAOControladorCasosNegocioImpl imp = new DAOControladorCasosNegocioImpl();
            String titulo = "implementacion DAO Controlador de Casos de Negocio";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);

            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton15.isSelected()) {
            DAOControladorOfertaEquipoImpl imp = new DAOControladorOfertaEquipoImpl();
            String titulo = "implementacion DAO Controlador de Oferta - Equipo";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton16.isSelected()) {
            DAOControladorRTPImpl imp = new DAOControladorRTPImpl();
            String titulo = "implementacion DAO Controlador RPT";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton17.isSelected()) {
            DAODescuentoAgregacionImpl imp = new DAODescuentoAgregacionImpl();

            String titulo = "implementacion DAO Controlador RPT";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton18.isSelected()) {
            DAOSeqDescAgregacionImpl imp = new DAOSeqDescAgregacionImpl();
            String titulo = "implementacion DAO Secuencia de Descuentos de Agregación";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton19.isSelected()) {
            DAOEventosTiempoEjecucionImpl imp = new DAOEventosTiempoEjecucionImpl();
            String titulo = "implementacion DAO Eventos de tiempo de ejecución";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton20.isSelected()) {
            DAOFallasGeneralesImpl imp = new DAOFallasGeneralesImpl();
            String titulo = "implementacion DAO Fallas Generales";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton21.isSelected()) {
            DAOGrupoPolizasImpl imp = new DAOGrupoPolizasImpl();
            String titulo = "implementacion DAO Grupo de Pólizas";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton22.isSelected()) {
            DAOIndustriasImpl imp = new DAOIndustriasImpl();
            String titulo = "implementacion DAO Industrias";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton23.isSelected()) {
            DAOInterciudadesImpl imp = new DAOInterciudadesImpl();
            String titulo = "implementacion DAO Interciudades";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton24.isSelected()) {
            DAOListaCompañiasImpl imp = new DAOListaCompañiasImpl();
            String titulo = "implementacion DAO Lista de Compañias";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton25.isSelected()) {
            DAOListaPolizasImpl imp = new DAOListaPolizasImpl();
            String titulo = "implementacion DAO Listas de Pólizas (Pólizas de flujo de trabajo)";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton26.isSelected()) {
            DAOListaPreciosImpl imp = new DAOListaPreciosImpl();
            String titulo = "implementacion DAO Lista de Precios";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton28.isSelected()) {
            DAOListaValoresImpl imp = new DAOListaValoresImpl();
            String titulo = "implementacion DAO Lista de Valores";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton29.isSelected()) {
            DAOMatricesElegibilidadCompImpl imp = new DAOMatricesElegibilidadCompImpl();
            String titulo = "implementacion DAO Matrices de Elegibilidad y Compatibilidad";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton30.isSelected()) {
            DAOMensajesValidacionImpl imp = new DAOMensajesValidacionImpl();
            String titulo = "implementacion DAO Mensajes de validación";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton31.isSelected()) {
            DAOModelosEstadostransPuestoTrabajoImpl imp = new DAOModelosEstadostransPuestoTrabajoImpl();
            String titulo = "implementacion DAO Modelos de Estado, Transacciones y Puestos de trabajo";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton32.isSelected()) {
            DAOOficinasOtrosMediosPagoImpl imp = new DAOOficinasOtrosMediosPagoImpl();
            String titulo = "implementacion DAO Oficinas – Otros Medios de Pago";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";

            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton33.isSelected()) {
            DAOCatalogoProductoPlazoForzosoImpl imp = new DAOCatalogoProductoPlazoForzosoImpl();
            String titulo = "implementacion DAO Catalogo de productos y plazo forzoso";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";
            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
        if (jRadioButton34.isSelected()) {
            DAOPuntosInventarioImpl imp = new DAOPuntosInventarioImpl();
            String titulo = "implementacion DAO Todos los Puntos de Inventario";
            Reportes rep = new Reportes();
            rep.agregarTextoAlfinal(titulo);
            imp.getHora();

            try {
                imp.ConectarDB(urlEx, usuarioEx, passEx);

            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
//                imp.inserta(imp.consultaPadre(fechaIni, fechaTer, usuario, version, ambienteInser, ambienteExtra), iteraciones, fechaIni, fechaTer, urlIn, usuarioIn, passIn, usuario, version, ambienteInser, ambienteExtra,usuEsq);
            } catch (Exception ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
                String mensaje = "el error es:" + ex.getMessage();
                rep.agregarTextoAlfinal(mensaje);
            }
            try {
                imp.CloseDB();
            } catch (SQLException ex) {
                Logger.getLogger(Por_Documento.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Termino correcto implementacion DAO");
            String mensaje = "Termino correcto implementacion DAO";
            rep.agregarTextoAlfinal(mensaje);
            imp.getHora();
            String linea = "--------------------------------------------------------------------------";
            rep.agregarTextoAlfinal(linea);
        }
 
        File ruta = new File("");
        JOptionPane.showMessageDialog(this, "el reporte se creo en le sigiente ruta: " + ruta.getAbsolutePath() + "\\ReporteSeteo.txt");


    }//GEN-LAST:event_jButton2ActionPerformed

    private void jButton3ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton3ActionPerformed
        Ejecuta_Todo prin = new Ejecuta_Todo();
        prin.setVisible(true);
        this.setVisible(false);


    }//GEN-LAST:event_jButton3ActionPerformed

    private void jButton1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jButton1ActionPerformed
        System.exit(0);
    }//GEN-LAST:event_jButton1ActionPerformed

    private void jRadioButton1ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jRadioButton1ActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jRadioButton1ActionPerformed

    private void jRadioButton13ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jRadioButton13ActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jRadioButton13ActionPerformed

    private void jRadioButton23ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jRadioButton23ActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jRadioButton23ActionPerformed

    private void jRadioButton2ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jRadioButton2ActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jRadioButton2ActionPerformed

    private void jRadioButton12ActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_jRadioButton12ActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_jRadioButton12ActionPerformed

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        /* Set the Nimbus look and feel */
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
            java.util.logging.Logger.getLogger(Por_Documento.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(Por_Documento.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(Por_Documento.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(Por_Documento.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>
        //</editor-fold>
        //</editor-fold>
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            @Override
            public void run() {
                new Por_Documento().setVisible(true);
            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.ButtonGroup buttonGroup1;
    private javax.swing.JButton jButton1;
    private javax.swing.JButton jButton2;
    private javax.swing.JButton jButton3;
    private javax.swing.JPanel jPanel2;
    private javax.swing.JPanel jPanel5;
    private javax.swing.JRadioButton jRadioButton1;
    private javax.swing.JRadioButton jRadioButton10;
    private javax.swing.JRadioButton jRadioButton11;
    private javax.swing.JRadioButton jRadioButton12;
    private javax.swing.JRadioButton jRadioButton13;
    private javax.swing.JRadioButton jRadioButton14;
    private javax.swing.JRadioButton jRadioButton15;
    private javax.swing.JRadioButton jRadioButton16;
    private javax.swing.JRadioButton jRadioButton17;
    private javax.swing.JRadioButton jRadioButton18;
    private javax.swing.JRadioButton jRadioButton19;
    private javax.swing.JRadioButton jRadioButton2;
    private javax.swing.JRadioButton jRadioButton20;
    private javax.swing.JRadioButton jRadioButton21;
    private javax.swing.JRadioButton jRadioButton22;
    private javax.swing.JRadioButton jRadioButton23;
    private javax.swing.JRadioButton jRadioButton24;
    private javax.swing.JRadioButton jRadioButton25;
    private javax.swing.JRadioButton jRadioButton26;
    private javax.swing.JRadioButton jRadioButton28;
    private javax.swing.JRadioButton jRadioButton29;
    private javax.swing.JRadioButton jRadioButton3;
    private javax.swing.JRadioButton jRadioButton30;
    private javax.swing.JRadioButton jRadioButton31;
    private javax.swing.JRadioButton jRadioButton32;
    private javax.swing.JRadioButton jRadioButton33;
    private javax.swing.JRadioButton jRadioButton34;
    private javax.swing.JRadioButton jRadioButton4;
    private javax.swing.JRadioButton jRadioButton6;
    private javax.swing.JRadioButton jRadioButton7;
    private javax.swing.JRadioButton jRadioButton8;
    private javax.swing.JRadioButton jRadioButton9;
    private javax.swing.JScrollPane jScrollPane1;
    // End of variables declaration//GEN-END:variables
}
