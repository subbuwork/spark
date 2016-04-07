package com.scala.spark.streaming

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import java.net.Socket
import java.io.BufferedReader
import java.io.InputStreamReader

class CustomReceiver1(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2){
  def onStart(){
    new Thread("Socket Thread"){
      override def run(){
        receiver()           
      }
    }.start()
  }
  
  def onStop(){
    
  }
  
  def receiver(){
    var socket:Socket = null
    var userInput:String = null
    try{
      socket = new Socket(host,port)
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),"UTF-8"))
      userInput = reader.readLine()
      while(!isStopped() && userInput!=null){
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()
    }catch{
      case e: java.net.ConnectException=> restart("Error connecting::"+host+"::"+port,e)
      case t :Throwable => restart("Errror receiving data::",t)
    }
    
  }
  
}