package com.duansky.learning.join

import java.io.File

/**
  * Created by DuanSky on 2016/6/17.
  */
object JoinUtil {

  def deleteDir(path:String): Unit ={
    val file:File = new File(path)
    if(file.isFile) {
      file.delete
      return
    }
    else
      file.list.foreach(child => deleteDir(path + File.separator + child))
    file.delete
  }

}
