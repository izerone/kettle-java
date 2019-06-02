
package rep;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;

/**
 * Created by 31767 on 2017/8/16.
 */
public class KettleEnvironments {
  
  public static KettleDatabaseRepository repository;
  public static DatabaseMeta databaseMeta;
  public static KettleDatabaseRepositoryMeta kettleDatabaseMeta;
  public static RepositoryDirectoryInterface directory;

  /*
   * KETTLE初始化
   */
  public static String KettleEnvironments() {

    System.out.println("KettleEnvironments-->>>>>>>>>>>>>>>>>>>>>>>>");

    try {
      
      KettleEnvironment.init();
      repository = new KettleDatabaseRepository();
      databaseMeta = new DatabaseMeta("kettle-rep-1", "Mysql", "Native", "10.19.160.222","kettle-rep", "3306", "root", "root");
      kettleDatabaseMeta = new KettleDatabaseRepositoryMeta("ETL", "ERP","Transformation description", databaseMeta);
      repository.init(kettleDatabaseMeta);
      repository.connect("admin", "admin");// 资源库用户名和密码
      directory = repository.loadRepositoryDirectoryTree();
      
    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }
    return null;
  }

}

