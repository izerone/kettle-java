
package com.encdata.cn.service;

import org.pentaho.di.trans.TransMeta;

/**.
 *
 * ClassName: KettleService.java
 * Function: 
 * @date 2019年5月31日
 * @author lihao
 * @Since JDK1.8
 */
public interface KettleService {
  
  public TransMeta getTableInput();
  
  public TransMeta getTbaleOutput();

}
