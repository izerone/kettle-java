import com.encdata.cn.util.KettleUtils;


import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.io.FileUtils;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.insertupdate.InsertUpdateMeta;
import org.pentaho.di.trans.steps.systemdata.SystemDataMeta;
import org.pentaho.di.trans.steps.systemdata.SystemDataTypes;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;

public class SecondTransDemo {

  public static String bjdt_tablename = "t1";
  public static String kettle_tablename = "t1";

  public static final String[] databasesXML = {
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
          "<connection>" +
          "<name>bjdt</name>" +
          "<server>10.37.149.191</server>" +
          "<type>MYsqL</type>" +
          "<access>Native</access>" +
          "<database>jkl</database>" +
          "<port>3306</port>" +
          "<username>root</username>" +
          "<password>123456</password>" +
          "</connection>",
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
          "<connection>" +
          "<name>kettle</name>" +
          "<server>10.37.149.229</server>" +
          "<type>HIVE2</type>" +
          "<access>Native</access>" +
          "<database>lh</database>" +
          "<port>10000</port>" +
          "<username>hive</username>" +
          "<password>hive</password>" +
          "</connection>"};
  
  public static KettleDatabaseRepository repository;
  public static DatabaseMeta databaseMeta;
  public static KettleDatabaseRepositoryMeta kettleDatabaseMeta;
  public static RepositoryDirectoryInterface directory;

  public static void main(String[] args) {
    try {
      KettleEnvironment.init();
      
      /**
       * 获取资源库相关对象
       */
      repository = new KettleDatabaseRepository();
      databaseMeta = new DatabaseMeta("kettle-rep-1", "Mysql", "Native", "10.19.160.222","kettle-rep", "3306", "root", "root");
      kettleDatabaseMeta = new KettleDatabaseRepositoryMeta("ETL", "ERP","Transformation description", databaseMeta);
      repository.init(kettleDatabaseMeta);
      repository.connect("admin", "admin");// 资源库用户名和密码
      directory = repository.loadRepositoryDirectoryTree();
 
      TransMeta transMeta = generateMyOwnTrans11();
      
      Trans trans = new Trans(transMeta); 
      
      trans.execute(null);
      
      trans.waitUntilFinished();
      
      /**
       * 添加信息到资源库
       */
      
      /*System.out.println("Saving ----------------------------------------------");
      
      RepositoryDirectoryInterface directory = repository.findDirectory("/");// 获取要保存的目录
      transMeta.setRepositoryDirectory(directory);// 设置目录
      repository.save(transMeta, "save_from_java_code");// 保存transformation到资源库
      
      
      
      String transXml = transMeta.getXML();
      // System.out.println("transXml:"+transXml);
      String transName = "C://update_insert_Trans2.ktr";
      File file = new File(transName);
      FileUtils.writeStringToFile(file, transXml, "UTF-8");*/

      // System.out.println(databasesXML.length+"\n"+databasesXML[0]+"\n"+databasesXML[1]);
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }

  }

  /**
   * 生成一个转化,把一个数据库中的数据转移到另一个数据库中,只有两个步骤,第一个是表输入,第二个是表插入与更新操作
   * 
   * @return
   * @throws KettleXMLException
   */
  public static TransMeta generateMyOwnTrans11() throws KettleXMLException {

    System.out.println("************start to generate my own transformation***********");

    TransMeta transMeta = new TransMeta();

    // 设置转化的名称
    transMeta.setName("mysql_hive_3");

    // 添加转换的数据库连接
    for (int i = 0; i < databasesXML.length; i++) {
      DatabaseMeta databaseMeta = new DatabaseMeta(databasesXML[i]);
      transMeta.addDatabase(databaseMeta);
    }

    // registry是给每个步骤生成一个标识Id用
    PluginRegistry registry = PluginRegistry.getInstance();

    // ******************************************************************

    // 第一个表输入步骤(TableInputMeta
    TableInputMeta tableInput = new TableInputMeta();
    String tableInputPluginId = registry.getPluginId(StepPluginType.class, tableInput);
    // 给表输入添加一个DatabaseMeta连接数据库
    DatabaseMeta database_bjdt = transMeta.findDatabase("bjdt");
    tableInput.setDatabaseMeta(database_bjdt);
    String select_sql =
        "SELECT ID,NAME FROM " + bjdt_tablename;
    tableInput.setSQL(select_sql);

    // 添加TableInputMeta到转换中
    //StepMeta tableInputMetaStep = new StepMeta(tableInputPluginId, "table input", tableInput);

    // 给步骤添加在spoon工具中的显示位置
    //tableInputMetaStep.setDraw(true);
    //tableInputMetaStep.setLocation(100, 100);

    //transMeta.addStep(tableInputMetaStep);

    System.out.println("Starting--获取输出插件");
    
    TableOutputMeta tableOutput = new TableOutputMeta();
    String tableOutputPluginId = registry.getPluginId(StepPluginType.class, tableOutput);
    
    // 给表输出添加一个DatabaseMeta连接数据库
    DatabaseMeta database_kettle = transMeta.findDatabase("kettle");
    
    tableOutput.setDatabaseMeta(database_kettle);
    
    tableOutput.setTableName("t1");
    
    tableOutput.setCommitSize(100);
    
    tableOutput.setFieldDatabase(new String[]{"id", "name"});
    tableOutput.setFieldStream(new String[]{"id", "name"});

    // 添加TableInputMeta到转换中
    StepMeta tableOutputMetaStep = new StepMeta(tableOutputPluginId, "t1" + "_table_output", tableOutput);

    transMeta.addStep(tableOutputMetaStep);
    
    System.out.println("Endinging--获取输出");

    // ******************************************************************
    // 添加hop把两个步骤关联起来
    //transMeta.addTransHop(new TransHopMeta(tableInputMetaStep, tableOutputMetaStep));
    System.out.println("***********the end************");
    
    // 开始执行转换
    System.out.println("Starting------------------------------------------------------------------");

    /*try {
      
      Trans trans = new Trans(transMeta); 
      
      trans.execute(null);
      
      trans.waitUntilFinished();
      
    } catch (KettleException e) {
      e.printStackTrace();
    } */
    
    // 结束执行转换
    System.out.println("Ending------------------------------------------------------------------");
    
    
    
    return transMeta;
  }
}
