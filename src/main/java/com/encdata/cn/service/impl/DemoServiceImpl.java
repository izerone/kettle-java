
package com.encdata.cn.service.impl;

import com.encdata.cn.service.DemoService;

import java.beans.DesignMode;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**.
 *
 * ClassName: DemoServiceImpl.java
 * Function: 
 * @date 2019年5月31日
 * @author lihao
 * @Since JDK1.8
 */
public class DemoServiceImpl implements DemoService{
  
  public static void main(String[] args) {
    
    Demo1 demo1 = new Demo1("username1", "password1");
    
    Demo2 demo2 = new Demo2();
    
    Class<?> sourceClass = demo1.getClass();
    Field[] sourceFiled = sourceClass.getDeclaredFields();
    for (Field field : sourceFiled) {
      field.setAccessible(true);
      try {
        String fieldName = field.getName();
        String setterMethodName = "set" + fieldName.substring(0, 1).toUpperCase()+fieldName.substring(1);
        Method m = demo2.getClass().getMethod(setterMethodName, String.class);
        m.invoke(demo2, field.get(demo1));
      } catch (IllegalArgumentException e) {
        e.printStackTrace();
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      } catch (NoSuchMethodException e) {
        e.printStackTrace();
      } catch (SecurityException e) {
        e.printStackTrace();
      } catch (InvocationTargetException e) {
        e.printStackTrace();
      }
    }
    System.out.println(demo2.getUsername() + demo2.getPassword()); 
  }
}

class Demo1 {
  
  private String username;
  
  private String password;
  
  public Demo1(String username, String password) {
    this.username = username;
    this.password = password;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }
}

class Demo2 {
  
  private String username;
  
  private String password;

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }
}
