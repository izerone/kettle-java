
package com.encdata.cn.util;

import com.encdata.cn.dto.DataBaseInfo;
import com.encdata.cn.plugins.HiveTableOutputMeta;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
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
import org.pentaho.di.trans.steps.tableoutput.TableOutput;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;

/**.
 *
 * ClassName: getKettleDatabaseRepository.java
 * Function: 
 * @date 2019年5月30日
 * @author lihao
 * @Since JDK1.8
 */
public class KettleUtils {
  
  public static String transName = "input_output_lihao_10";
  
  public static String sourceTableColumns = "";
  
  public static String[] columns = new String[13];
  
  public static String sourceTableName = "table1";
  
  public static String targetTableColumns = "id,name";
  
  public static String targetTableName = "table1";
  
  public static DatabaseMeta sourceDatabaseMeta;
  
  public static DatabaseMeta sinkDatabaseMeta;
  
  public static StepMeta tableInputMetaStep;
  
  public static StepMeta systemDataMetaStep;
  
  public static StepMeta tableOutputMetaStep;
  
  public static StepMeta insertUpdateStep;
  
  public static final String[] databasesXML = {
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
          "<connection>" +
          "<name>bjdt</name>" +
          "<server>10.37.149.191</server>" +
          "<type>Mysql</type>" +
          "<access>Native</access>" +
          "<database>jkl</database>" +
          "<port>3306</port>" +
          "<username>root</username>" +
          "<password>123456</password>" +
          "</connection>",
          /*"<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
              "<connection>" +
              "<name>kettle</name>" +
              "<server>10.37.149.117</server>" +
              "<type>Oracle</type>" +
              "<access>Native</access>" +
              "<database>orcl2</database>" +
              "<port>1521</port>" +
              "<username>lihao1</username>" +
              "<password>lihao1</password>" +
              "</connection>"*/
  /*"<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
          "<connection>" +
          "<name>kettle</name>" +
          "<server>10.37.149.116</server>" +
          "<type>mysql</type>" +
          "<access>Native</access>" +
          "<database>circles</database>" +
          "<port>3306</port>" +
          "<username>root</username>" +
          "<password>admin123</password>" +
          "</connection>"*/
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
          "</connection>"
          
          };
  
  public static KettleDatabaseRepository repository;
  public static DatabaseMeta databaseMeta;
  public static KettleDatabaseRepositoryMeta kettleDatabaseMeta;
  public static RepositoryDirectoryInterface directory;
  public static TransMeta transMeta;
  
  public static PluginRegistry registry = PluginRegistry.getInstance();
  
  public static KettleDatabaseRepository getConn() {
    
    for (int i=1;i<14;i++) {
      sourceTableColumns += "name" + i;
      if (i != 13) {
        sourceTableColumns += ",";
      }
    }
    
    for (int i=1;i<14;i++) {
      columns[i-1]="name" + i;
    }
    
    System.out.println("Starting--环境初始化");
    
    try {
      
      KettleEnvironment.init();
      
      /**
       * 获取资源库相关对象
       */
      repository = new KettleDatabaseRepository();
      databaseMeta = new DatabaseMeta("", "Mysql", "Native", "10.19.160.222","kettle-rep", "3306", "root", "root");
      kettleDatabaseMeta = 
          new KettleDatabaseRepositoryMeta("", "","Transformation description", databaseMeta);
      repository.init(kettleDatabaseMeta);
      repository.connect("admin", "admin");
      
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    System.out.println("Endinging--环境初始化");
    
    return repository;
  } 
  
  public static void initSourceDatabaseMeta(DataBaseInfo databaseInfo) {
    sourceDatabaseMeta = new DatabaseMeta(databaseInfo.getName(), databaseInfo.getType(), 
        databaseInfo.getAccess(), databaseInfo.getDb(),
        databaseInfo.getHost(), databaseInfo.getPort(),
        databaseInfo.getUser(), databaseInfo.getPassword());
  }
  
  public static void initSinkDatabaseMeta(DataBaseInfo databaseInfo) {
    sinkDatabaseMeta = new DatabaseMeta(databaseInfo.getName(), databaseInfo.getType(), 
        databaseInfo.getAccess(), databaseInfo.getDb(),
        databaseInfo.getHost(), databaseInfo.getPort(),
        databaseInfo.getUser(), databaseInfo.getPassword());
  }
  
  public static boolean addTransToRep() {
    
    System.out.println("Starting--天剑转换到资源库");
    
    try {
      // 获取要保存的目录
      RepositoryDirectoryInterface directory = repository.findDirectory("/");
      // 设置目录
      transMeta.setRepositoryDirectory(directory);
      // 保存transformation到资源库
      repository.save(transMeta, "save_from_java_code");
      
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    System.out.println("Endinging--添加资源库");
    
    return true;
    
  }
  
  public static boolean excuteTrans() {
    
    System.out.println("Starting--执行转换");
    
    try {
      
      StepMeta stepMeta = (StepMeta)transMeta.getTransHop(0).getFromStep();
      
      System.out.println(stepMeta.getName());
      
      Trans trans = new Trans(transMeta);  

      trans.execute(null); 
      
      trans.waitUntilFinished(); 
      
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    System.out.println("Endinging--执行转换");
    
    return true;
  }
  
  public static TransMeta getTrans() {
    
    System.out.println("Starting--获取转换");
    
    try {
      
      transMeta = new TransMeta();
      
      transMeta.setName(transName);

      // 添加转换的数据库连接
      for (int i = 0; i < databasesXML.length; i++) {
        DatabaseMeta databaseMeta = new DatabaseMeta(databasesXML[i]);
        transMeta.addDatabase(databaseMeta);
      }
      
      getTableInput();
      
      //getSystemDataMeta();
      
      //transMeta.addTransHop(new TransHopMeta(tableInputMetaStep, systemDataMetaStep));
      
      getTbaleOutput();
      
      transMeta.addTransHop(new TransHopMeta(tableInputMetaStep, tableOutputMetaStep));
      
      Trans trans = new Trans(transMeta);  

      trans.execute(null); 
      
      trans.waitUntilFinished();

    } catch (Exception e) {
      e.printStackTrace();
    }
    
    System.out.println("Endinging--获取转换");
    
    return transMeta;
  } 
  
  public static void getTableInput() {
    
    System.out.println("Starting--表输入插件");
    
    // 第一个表输入步骤(TableInputMeta
    TableInputMeta tableInput = new TableInputMeta();
    String tableInputPluginId = registry.getPluginId(StepPluginType.class, tableInput);
    // 给表输入添加一个DatabaseMeta连接数据库
    DatabaseMeta database_bjdt = transMeta.findDatabase("bjdt");
    tableInput.setDatabaseMeta(database_bjdt);
    String select_sql =
        " SELECT " + sourceTableColumns + " FROM t1";
    tableInput.setSQL(select_sql);

    // 添加TableInputMeta到转换中
    tableInputMetaStep = new StepMeta(tableInputPluginId, "T1" + "_table_input", tableInput);

    transMeta.addStep(tableInputMetaStep);
    
    System.out.println("Endinging--表输入插件"); 
  }
  
  public static void getInsertOrUpdate() {
    
    System.out.println("Starting--插入与更新插件");
    
    InsertUpdateMeta insertUpdateMeta = new InsertUpdateMeta();
    String insertUpdateMetaPluginId = registry.getPluginId(StepPluginType.class, insertUpdateMeta);
    // 添加数据库连接
    DatabaseMeta database_kettle = transMeta.findDatabase("kettle");
    insertUpdateMeta.setDatabaseMeta(database_kettle);
    // 设置操作的表
    insertUpdateMeta.setTableName(targetTableName);

    // 设置用来查询的关键字
    insertUpdateMeta.setKeyLookup(new String[] {"ID"});
    insertUpdateMeta.setKeyStream(new String[] {"ID"});
    insertUpdateMeta.setKeyStream2(new String[] {""});// 一定要加上
    insertUpdateMeta.setKeyCondition(new String[] {"="});

    // 设置要更新的字段
    String[] updatelookup = {"ID","NAME"};
    String[] updateStream = {"ID","NAME"};
    Boolean[] updateOrNot = {false, false};
    insertUpdateMeta.setUpdateLookup(updatelookup);
    insertUpdateMeta.setUpdateStream(updateStream);
    insertUpdateMeta.setUpdate(updateOrNot);
    String[] lookup = insertUpdateMeta.getUpdateLookup();
    // System.out.println("******:"+lookup[1]);
    // System.out.println("insertUpdateMetaXMl:"+insertUpdateMeta.getXML());
    // 添加步骤到转换中
    StepMeta insertUpdateStep =
        new StepMeta(insertUpdateMetaPluginId, targetTableName + "_insert_update", insertUpdateMeta);
    insertUpdateStep.setDraw(true);
    insertUpdateStep.setLocation(250, 100);
    transMeta.addStep(insertUpdateStep);
    
    System.out.println("Endinging--插入与更新插件");
  }
  
  public static void getTbaleOutput() {
    
    System.out.println("Starting--表输出插件");
    
    //TableOutputMeta tableOutput = new TableOutputMeta();
    
    HiveTableOutputMeta tableOutput = new HiveTableOutputMeta();
    
    String tableOutputPluginId = registry.getPluginId(StepPluginType.class, tableOutput);
    
    // 给表输出添加一个DatabaseMeta连接数据库
    DatabaseMeta database_kettle = transMeta.findDatabase("kettle");
    
    tableOutput.setDatabaseMeta(database_kettle);
    
    tableOutput.setTableName("t2");
    
    System.out.println("aaaa");
    
    tableOutput.setFieldDatabase(columns);
    tableOutput.setFieldStream(columns);

    System.out.println("bbbb");
    
    // 添加TableInputMeta到转换中
    tableOutputMetaStep = new StepMeta(tableOutputPluginId, targetTableName + "_table_output", tableOutput);
 
    transMeta.addStep(tableOutputMetaStep);
    
    System.out.println("Endinging--获取表输出插件");
  }
  
  public static void getSystemDataMeta() {
    
    System.out.println("Starting--获取系统信息插件");
    
    SystemDataMeta systemDataMeta = new SystemDataMeta();
    String systemDataMetaPluginId = registry.getPluginId(StepPluginType.class, systemDataMeta);
    systemDataMeta.setFieldName(new String[]{"enn_timestamp"});
    systemDataMeta.setFieldType(new SystemDataTypes[]{SystemDataTypes.TYPE_SYSTEM_INFO_NONE});
    
    systemDataMetaStep = new StepMeta(systemDataMetaPluginId, "system data meta", systemDataMeta);
    
    transMeta.addStep(systemDataMetaStep);

  }
  
  public static void main(String[] args) {
    
    getConn();
    
    getTrans();
    
    //addTransToRep();
    
    //excuteTrans();
    
  }

}
