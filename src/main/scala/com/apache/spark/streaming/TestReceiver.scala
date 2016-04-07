package com.apache.spark.streaming

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import java.net.Socket
import java.io.BufferedReader
import java.io.InputStreamReader

class TestReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2){
  
  def onStart(){
    new Thread("Socket Receiver"){
      override def run(){
        receiver()
      }
    }.start()
  }
  
  def onStop(){
    // There is nothing much to do as the thread calling receive()
   // is designed to stop by itself if isStopped() returns false
  }
  
  def receiver(){
    
    var socket     : Socket = null
    var userIinput : String = null
    
    try{
        // connecting to host:port
        socket     = new Socket(host,port)
        val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),"UTF-8"))
        userIinput = reader.readLine()
        while(!isStopped()&& userIinput != null){
          store(userIinput)
          userIinput = reader.readLine()
        }
        reader.close()
        socket.close()
        restart("Trying to connect again!!!!!!!!!")
    }catch{
      case e :java.net.ConnectException =>
        restart("Error connecting to " + host + ":" + port, e)
      case t : Throwable =>
        restart("Error receiving data", t)
    }
    
  }
  
}