package mysql;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;


public class ManageMysql
{
  private boolean error = false;
  private String mssg;
  
  public ManageMysql() {}
  
  public Connection Connect() { Connection link = null;

    String username, password , driver , server , bdd, url;

    try
    {
      Properties p = new Properties();
      p.load(new FileInputStream("/opt/lampp/htdocs/conf.ini"));

      username = p.getProperty("username");
      password = p.getProperty("password");
      driver = p.getProperty("driver");
      server = p.getProperty("server");
      bdd = p.getProperty("database");
      url = driver + "://" + server + "/" + bdd;
    } catch (Exception e) {
      mssg = ("error read file ini:" + e.getMessage());
      error = true;
      return link; 
    }
    
    try {
      Class.forName("com.mysql.jdbc.Driver");
      link = DriverManager.getConnection(url, username, password);
    }
    catch (ClassNotFoundException cnfe)
    {
      mssg = "Error class not found";
      error = true;
    }
    catch (SQLException sqle)
    {
      getErrorMsg(sqle);
      error = true;
    }
    catch (Exception e)
    {
      mssg = ("Error :" + e.getMessage());
      error = true;
    }
    
    return link;
  }
  
  public Connection ConnectDB(String username, String password, String url)
  {
    Connection link = null;
    
    try
    {
      Class.forName("com.mysql.jdbc.Driver");
      link = DriverManager.getConnection(url, username, password);
    }
    catch (ClassNotFoundException cnfe)
    {
      mssg = "Error class not found";
      error = true;
    }
    catch (SQLException sqle)
    {
      getErrorMsg(sqle);
      error = true;
    }
    catch (Exception e)
    {
      mssg = ("Error :" + e.getMessage());
      error = true;
    }
    return link;
  }
  
  public void DisConnect(Connection link)
  {
    try
    {
      link.close();
    } catch (SQLException localSQLException) {}
  }
  
  public PreparedStatement GetQuery(Connection link, String query) {
    PreparedStatement st = null;
    try {
      st = link.prepareStatement(query, 1);
    }
    catch (SQLException sqle) {
      getErrorMsg(sqle);
      error = true;
    }
    return st;
  }
  
  private void freeQuery(PreparedStatement st, ResultSet result) {
    try {
      st.close();
      result.close();
    }
    catch (SQLException localSQLException) {}
  }
  

  public int getErrorNum(SQLException sqle)
  {
    return sqle.getErrorCode();
  }
  
  public void getErrorMsg(SQLException sqle) {
    int num_error = sqle.getErrorCode();
    
    String numerror = Integer.toString(num_error);
    
    mssg = ("Error:" + numerror + " " + sqle.getMessage());
  }
  
  private String typecol;
  private int numrows;
  public HashMap<String, Object> loadResult(PreparedStatement st)
  {
    ResultSet result = null;
    HashMap<String, Object> tabvalue = null;
    try
    {
      result = st.executeQuery();
      if (!result.next()) {
        numrows = 0;
      }
      else {
        String namecol = result.getMetaData().getColumnName(1);
        
        Object value = result.getObject(1);
        

        if (value == null) {
          mssg = ("Column " + typecol + " not exist");
          tabvalue = null;
          error = true;
        } else {
          tabvalue = new HashMap();
          
          tabvalue.put(namecol, value);
          numrows = 1;
        }
        result.close();
      }
      freeQuery(st, result);
    }
    catch (SQLException sqle) {
      getErrorMsg(sqle);
      error = true;
    }
    return tabvalue;
  }
  
  public HashMap<String, Object> loadAssoc(PreparedStatement st)
  {
    ResultSet result = null;
    HashMap<String, Object> tabvalue = null;
    try {
      result = st.executeQuery();
      if (!result.next()) {
        numrows = 0;
      }
      else {
        tabvalue = new HashMap();
        
        for (int index = 1; index <= result.getMetaData().getColumnCount(); index++) {
          String namecol = result.getMetaData().getColumnName(index);
          Object value = result.getObject(index);
          

          if (value == null) {
            mssg = ("Column " + typecol + " not exist");
            tabvalue = null;
            error = true;
            break;
          }
          tabvalue.put(namecol, value);
        }
        
        result.close();
        numrows = 1;
      }
      freeQuery(st, result);
    } catch (SQLException sqle) {
      getErrorMsg(sqle);
      error = true;
    }
    return tabvalue;
  }
  
  public ArrayList<HashMap<String, Object>> loadAssocList(PreparedStatement st)
  {
    ResultSet result = null;
    HashMap<String, Object> tabvalue = null;
    ArrayList<HashMap<String, Object>> listtabvalue = new ArrayList();
    numrows = 0;
    try
    {
      result = st.executeQuery();
      



      while (result.next()) {
        tabvalue = new HashMap();
        for (int index = 1; index <= result.getMetaData().getColumnCount(); index++) {
          String namecol = result.getMetaData().getColumnName(index);
          Object value = result.getObject(index);
          
          if (value == null) {
            mssg = ("Column " + typecol + " not exist");
            tabvalue = null;
            error = true;
            break;
          }
          tabvalue.put(namecol, value);
        }
        
        if (error) break;
        numrows += 1;
        listtabvalue.add(tabvalue);
      }
      result.close();
      freeQuery(st, result);
    } catch (SQLException sqle) {
      getErrorMsg(sqle);
      error = true;
    }
    return listtabvalue;
  }
  

  public int GetQuerynoSelect(Connection link, PreparedStatement st)
  {
    int result = 0;
    try
    {
      st.executeUpdate();
      ResultSet rs = st.getGeneratedKeys();
      if ((rs != null) && (rs.next())) {
        result = rs.getInt(1);
      }
      rs.close();
      st.close();
    } catch (SQLException sqle) {
      getErrorMsg(sqle);
      error = true;
    }
    return result;
  }
  
  public void SetVariables(PreparedStatement st, int index, Object var, int type) {
    try {
      st.setObject(index, var, type);
    } catch (SQLException sqle) {
      getErrorMsg(sqle);
      error = true;
    }
  }
  


  public boolean getError()
  {
    return error;
  }
  
  public String getMessgError()
  {
    return mssg;
  }
  
  public int getNumRow()
  {
    return numrows;
  }
}
