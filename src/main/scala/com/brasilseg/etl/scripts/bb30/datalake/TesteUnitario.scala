package com.brasilseg.etl.scripts.bb30.datalake

object TesteUnitario extends App {


  val sysArgs: Array[String] = Array("--redshift_key_path","3", "--r", "--5", "--6", "--7")

  if(sysArgs.mkString("Array(", ", ", ")").contains("redshift_key_path"))
    print("foi")

}
