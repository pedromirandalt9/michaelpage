
package co.gov.sistema.servicios;

import java.io.ByteArrayOutputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Local;
import javax.ejb.Remote;
import javax.ejb.Stateless;
import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import javax.jws.soap.SOAPBinding;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Query;
import javax.sql.DataSource;

import oracle.jdbc.OracleTypes;

import org.apache.axis.client.Stub;
import org.apache.commons.lang.time.DateUtils;
import com.itextpdf.kernel.pdf.filespec.PdfFileSpec;


@Stateless
public class ClienteEjb implements IClienteEjb {

	@PersistenceContext(unitName = "carCenterBD")
	private EntityManager emCarCenter;
	
	
	@Resource(mappedName = "java:carCenterDS", type = DataSource.class)
	private static DataSource dataSource;

	public ClienteEjb() {
	
	}
	
	//Consulta de Clientes que han comprado un acumulado $100.000 en los últimos 60 días
	public ArrayList<Cliente> getClientesAculumado() {
		Connection conn = null;
		CallableStatement stmt = null;
		ArrayList<Cliente> results = new ArrayList<Cliente>();
		ResultSet rs = null;
		try {
			String functionCall = "{call CER_CENTER.PF_SERVICES.PR_OBTENER_ACUMULADO(?)}";
			conn = dataSource.getConnection();
			stmt = conn.prepareCall(functionCall);
			stmt.registerOutParameter(1, OracleTypes.CURSOR);
			stmt.executeQuery();
			rs = (ResultSet) stmt.getObject(1);
			results = new ArrayList<Cliente>();
			while (rs.next()) {

				results.add(new Cliente(rs.getString("primer_apellido"),
						rs.getString("segundo_apellido"), rs.getLong("tipo_documento"),
						rs.getString("documento"));
			}

		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
			if (stmt != null)
				stmt.close();
			if (conn != null)
				conn.close();
			return null;
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
			return null;
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
				if (rs != null)
					rs.close();
				conn = null;
			} catch (SQLException se2) {
			}// nothing we can do
			try {
				if (conn != null)
					conn.close();
				conn = null;
				return results;
			} catch (SQLException se) {

				return null;
			}// end finally try
		}// end try
	
	}
	
	//6.	Consulta de los 100 productos más vendidos en los últimos 30 días
	public ArrayList<Producto> getProductosMasVendidos(String dias) {
		Connection conn = null;
		CallableStatement stmt = null;
		ArrayList<Producto> results = new ArrayList<Producto>();
		ResultSet rs = null;
		try {
			String functionCall = "{call CER_CENTER.PF_SERVICES.PR_PRODUCTOS_MAS_VENDIDOS(?,?)}";
			conn = dataSource.getConnection();
			stmt = conn.prepareCall(functionCall);
			stmt.setLong(1, dias);
			stmt.registerOutParameter(2, OracleTypes.CURSOR);
			stmt.executeQuery();
			rs = (ResultSet) stmt.getObject(2);
			results = new ArrayList<Producto>();
			while (rs.next()) {
				results.add(new Producto(rs.getString("precio"),
						rs.getString("unidades"), rs.getLong("descuento"),
						rs.getString("documento"));
			}

		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
			if (stmt != null)
				stmt.close();
			if (conn != null)
				conn.close();
			return null;
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
			return null;
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
				if (rs != null)
					rs.close();
				conn = null;
			} catch (SQLException se2) {
			}// nothing we can do
			try {
				if (conn != null)
					conn.close();
				conn = null;
				return results;
			} catch (SQLException se) {

				return null;
			}// end finally try
		}// end try
	
	}
	
	//7.	Consulta de las tiendas que han vendido más de 100 UND del producto 100 en los últimos 60 días.
	public ArrayList<Tienda> getTiendasVentas(Long idProducto) {
		Connection conn = null;
		CallableStatement stmt = null;
		ArrayList<Tienda> results = new ArrayList<Tienda>();
		ResultSet rs = null;
		try {
			String functionCall = "{call CER_CENTER.PF_SERVICES.PR_OBTENER_TIENDAS(?,?)}";
			conn = dataSource.getConnection();
			stmt.setLong(1, idProducto);
			stmt = conn.prepareCall(functionCall);
			stmt.registerOutParameter(2, OracleTypes.CURSOR);
			stmt.executeQuery();
			rs = (ResultSet) stmt.getObject(2);
			results = new ArrayList<Tienda>();
			while (rs.next()) {
				results.add(new Tienda(rs.getString("nombre"));
			}

		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
			if (stmt != null)
				stmt.close();
			if (conn != null)
				conn.close();
			return null;
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
			return null;
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
				if (rs != null)
					rs.close();
				conn = null;
			} catch (SQLException se2) {
			}// nothing we can do
			try {
				if (conn != null)
					conn.close();
				conn = null;
				return results;
			} catch (SQLException se) {

				return null;
			}// end finally try
		}// end try
	
	}
	//8.	Consulta de todos los clientes que han tenido más de un(1) mantenimento en los últimos 30 días.
	public ArrayList<Cliente> getClientesMantenimiento() {
		Connection conn = null;
		CallableStatement stmt = null;
		ArrayList<Cliente> results = new ArrayList<Cliente>();
		ResultSet rs = null;
		try {
			String functionCall = "{call CER_CENTER.PF_SERVICES.PR_OBTENER_CLIENTES_TIENDAS(?)}";
			conn = dataSource.getConnection();
			stmt = conn.prepareCall(functionCall);
			stmt.registerOutParameter(1, OracleTypes.CURSOR);
			stmt.executeQuery();
			rs = (ResultSet) stmt.getObject(5);
			results = new ArrayList<Cliente>();
			while (rs.next()) {
				results.add(new Cliente(rs.getString("primer_apellido"));
			}

		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
			if (stmt != null)
				stmt.close();
			if (conn != null)
				conn.close();
			return null;
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
			return null;
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
				if (rs != null)
					rs.close();
				conn = null;
			} catch (SQLException se2) {
			}// nothing we can do
			try {
				if (conn != null)
					conn.close();
				conn = null;
				return results;
			} catch (SQLException se) {

				return null;
			}// end finally try
		}// end try
	
	}
	
	//9.	Procedimiento que reste la cantidad de productos del inventario de las tiendas cada que se presente una venta.
	public ArrayList<Cliente> restarInventariosdeTienda(Long cantidad) {
		Connection conn = null;
		CallableStatement stmt = null;
		try {
			String functionCall = "{call ABC_PASILLO.PF_SERVICES.PR_RESTAR_INVENTARIO(?,?)}";
			conn = dataSource.getConnection();
			stmt = conn.prepareCall(functionCall);
			stmt.setLong(1, cantidad);
			stmt.registerOutParameter(2, Types.INTEGER);
			stmt.executeUpdate();
			int retorno = stmt.getInt(2);

			if (retorno > 0) {
				return true;
			} else {
				return false;
			}

		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
			return false;
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
			return false;
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();

			} catch (SQLException se2) {
			}// nothing we can do
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
				return false;
			}// end finally try
		}// end try
	
	}


}
