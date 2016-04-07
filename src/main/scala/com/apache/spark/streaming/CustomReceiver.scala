package com.apache.spark.streaming

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import java.net.Socket
import java.io.BufferedReader
import java.io.InputStreamReader

class CustomReceiver(host:String,port:Int) 
                    extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {
  
 def onStart(){
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
        override def   run(){
             receive()
           }
  }.start()
}
 
 def onStop(){
   // There is nothing much to do as the thread calling receive()
   // is designed to stop by itself if isStopped() returns false
 }
 
 def receive(){
   var socket:Socket = null
   var userInput: String = null
   
   try{
      //Connect to host:port
      socket = new Socket(host,port)
     
      // Until stopped or connection broken continue reading
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),"UTF-8"))
     
      userInput = reader.readLine()
     
      while(!isStopped() && userInput != null){
       store(userInput)
       userInput = reader.readLine()
       }
      reader.close()
      socket.close()
      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
      }catch{
     // restart if could not connect to server
     case e:java.net.ConnectException=>
       restart("Error connecting to " + host + ":" + port, e)
     case t:Throwable =>
        // restart if there is any other error
       restart("Error receiving data", t)
   }
 }
}