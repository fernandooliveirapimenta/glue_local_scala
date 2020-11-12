package com.brasilseg.etl.scripts.bb30.datalake

object TesteUnitario extends App {


  val sysArgs: Array[String] = Array("--redshift_key_path","3", "--r", "--5", "--6", "--7")

  if(sysArgs.mkString("Array(", ", ", ")").contains("redshift_key_path"))
    println("foi")

  val bucketCompleto: String = "s3://dev-brasilseg-segbr-extracao/chaves-redis/assistencia_ultron/assistencia"

  val splitado: Array[String] = bucketCompleto.split("/");
  //ex s3://dev-brasiseg-segbr-extracao
  val bucketPath: String = s"${splitado(0)}//${splitado(2)}"
  val arquivos: String = bucketCompleto.replace(bucketPath, "")
  println(bucketPath)
  println(arquivos)
  println(bucketCompleto.split("/").mkString("Array(", ", ", ")"))
  val a = bucketCompleto.split("/", 3);


}
