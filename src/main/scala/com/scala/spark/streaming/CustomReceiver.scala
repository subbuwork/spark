package com.scala.spark.streaming

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import java.net.Socket
import java.io.BufferedReader
import java.io.InputStreamReader

class CustomReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {
  
  def onStart(){
    new Thread("Socket Receiver"){
      override def run(){
        receiver()
      }
    }.start()
  }
  def onStop(){
    println("Stopping thread*****")
    
    
  }
  
  def receiver(){
    
    var socket:Socket =null
    var userInput:String = null
    try{
        println("host:"+host+"::"+port)
        socket = new Socket(host,port)
        val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),"UTF-8"))
        userInput = reader.readLine()
        println("Input::"+userInput)
        while(!isStopped() && userInput != null){
          println("insde while loop")
          store(userInput)
          userInput = reader.readLine()
        }
        reader.close()
        socket.close()
    }catch{
      case e : java.net.ConnectException=>restart("Error Connecting to "+host+":"+port,e)
      case t : Throwable=>restart("Error receiving data::",t)
    }
  }
  
}