
package main.scala

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.io.{BufferedSource, Source}
import scala.concurrent.{ExecutionContext,Future, future}
import scala.util.{Try,Failure, Success}

import javax.xml.bind.DatatypeConverter
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.concurrent.TimeUnit
import java.io.{BufferedWriter, File, FileWriter, PrintWriter,IOException}
import java.util.concurrent.{TimeUnit,ThreadPoolExecutor,ArrayBlockingQueue,LinkedBlockingQueue}

/**
 * @author Terry Adeagbo
 * This class extends [[main.scala.MTMessage]] interface.
 * @note The Parser Main Application.
 * @version 1.0
 */

object MTParserApp extends App with MTMessage{    
  System.setProperty("log.dir",args(3));
  isLoggingEnabled=true
  private val startDate=new Date()
  
  private val numWorkers = {
    System.getProperty("scala.concurrent.context.numThreads",
        sys.runtime.availableProcessors.toString()).toInt
  }
  
  private val queueCapacity = 1000
  private val thread= new ThreadPoolExecutor(
     numWorkers, numWorkers,
     0L, TimeUnit.SECONDS,
     new ArrayBlockingQueue[Runnable](queueCapacity,true),
     new ThreadPoolExecutor.CallerRunsPolicy
  )   
  //the ExecutionContext that wraps the thread pool
  private implicit val ec = ExecutionContext.fromExecutorService(thread )

  logger.debug("ARGUMENT[REGION][{}]",args(0))
  logger.debug("ARGUMENT[INPUT FILE][{}]",args(1))
  logger.debug("ARGUMENT[OUTPUT FOLDER][{}]",args(2))
  logger.debug("ARGUMENT[LOGS FOLDER][{}]",args(3))
  logger.debug("ARGUMENT[BICS FILE][{}]",args(4))
  logger.debug("ARGUMENT[COUNTRIES FILE][{}]",args(5))
  logger.debug("ARGUMENT[MESSAGES FILE][{}]",args(6))  
  logger.debug("ARGUMENT[REGIONS FILE][{}]",args(7))  
  
  private val suffix = "_ex"
  private val bufferSize =  32768//32KB
  private val inputFiles = HashMap.empty[String,Option[BufferedSource]]
  private val outputFiles = HashMap.empty[String,Option[PrintWriter]]  
  private val outputFileNames=HashMap.empty[String,String]
  private val message = ArrayBuffer[String]()
                                  
  private val region =args(0).trim.toUpperCase
  private val ondemandFile=new File(args(1))   
  private val outputDir = args(2) + File.separator   
  private val bicFilename=args(4)
  private val countryFilename=args(5)
  private val scopeFilename=args(6) 
  private val regionsFilename=args(7)
  
  private val prefix=ondemandFile.getName+"_datalake_" 

  private val messageStats = HashMap.empty[String,Int]  
  private val countryStats = HashMap.empty[String,Int]
  private val bicStats = HashMap.empty[String,Int]   
  
  private val headerStats = HashMap.empty[String,Int] 
  private val bodyStats = HashMap.empty[String,Int] 

  @volatile private var totalCount = 0  
  private var failedBodyCount = 0
  private var failedHeaderCount = 0
  private var dtcsCount = 0
  private var scopeCount = 0   
  private var lineIndex=0  
  private var exitStatus=0

  private def encode(text: String) = DatatypeConverter.printBase64Binary(text.getBytes())  

  private def getDateDiff(date1:Date,date2:Date,timeUnit:TimeUnit)= {
      val diffInMillies = date2.getTime() - date1.getTime()
      timeUnit.convert(diffInMillies,TimeUnit.MILLISECONDS)
  } 
  
  private def getFileStats(filenames: Array[String])={
    filenames.map {name=> 
      val file=new File(name)
      val size=file.length
      logger.info("[{}({})][FILE SIZE][{}]",file.getName,region,file.length.toString)
      size
    }.reduce(_+_)
  }
  
  private val reject = (i: String) => !i.trim.isEmpty && !i.startsWith("#")
  
  private def addToHeaderStats(message: String)={
      val msgType=if(message.isEmpty())None.toString.toUpperCase else message.toUpperCase 
      failedHeaderCount+=1
      if(!headerStats.contains(msgType)){
        headerStats+=(msgType->1)
      }else{
        val count=headerStats(msgType)+1
        headerStats+=(msgType->count)
      }   
  }  

  private def addToBodyStats(message: String)={
      val msgType=if(message.isEmpty())None.toString.toUpperCase else message.toUpperCase 
      failedBodyCount+=1
      if(!bodyStats.contains(msgType)){
        bodyStats+=(msgType->1)
      }else{
        val count=bodyStats(msgType)+1
        bodyStats+=(msgType->count)
      }   
  }   
  
  private def addToMessageStats(message: String)={
      val msgType=if(message.isEmpty())None.toString.toUpperCase else message.toUpperCase 
      if(!messageStats.contains(msgType)){
        messageStats+=(msgType->1)
      }else{
        val count=messageStats(msgType)+1
        messageStats+=(msgType->count)
      }   
  }
    
  private def addToCountryStats(country: String)={
      if(!countryStats.contains(country)){
        countryStats+=(country->1)
      }else{
        val count=countryStats(country)+1
        countryStats+=(country->count)
      }   
  }  
 
  private def addToBicStats(bic: String)={
      if(!bicStats.contains(bic)){
        bicStats+=(bic->1)
      }else{
        val count=bicStats(bic)+1
        bicStats+=(bic->count)
      }   
  } 
  
  private def closeInFiles={
      // input file can be close here      
      for ((name,file) <-inputFiles ) {
        if(file.isDefined){
          file.get.close
          logger.debug("FILE[{}({})][CLOSED]",name,region)
        }        
      }
  }
  private def closeOutFiles={   
      // output files can be close here
      for ((name,file) <-outputFiles ) {
        if(file.isDefined){
          file.get.flush
          file.get.close
          logger.debug("FILE[{}({})][CLOSED]",name,region)
        }        
      }      
  }
  
  private def getStatsPSV:String={
      val total=getFileStats(outputFileNames.values.toArray)
      val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") 
      val time = date.format(Calendar.getInstance().getTime()) 
      
      logger.info("[{}({})][OUTPUT FILE SIZE][{}]",ondemandFile.getName,region,total.toString)
 
      var statsPSV=time+"|"+region+"|"+ondemandFile.getName+"|"+ 
                   ondemandFile.getUsableSpace+"|"+ondemandFile.length+"|"+total+"|"+
                   totalCount+"|"+failedHeaderCount+"|"+failedBodyCount+"|"+
                   dtcsCount+"|"+scopeCount
              
      statsPSV+="|"+messageStats.map{
          case(key, value) =>key+":"+value
      }.mkString(";")
      
      statsPSV+="|"+countryStats.map{
          case(key, value) =>key+":"+value
      }.mkString(";")
      
      statsPSV+="|"+bicStats.map{
          case(key, value) =>key+":"+value
      }.mkString(";")
      
      statsPSV+="|"+headerStats.map{
          case(key, value) =>key+":"+value
      }.mkString(";")
      
      statsPSV+="|"+bodyStats.map{
          case(key, value) =>key+":"+value
      }.mkString(";")      
      
      statsPSV
  }
  
  private def logError(filename:String,region:String,errType:String,errMsg:String)={
    logger.error("[{}({})][{}][{}]",filename,region,errType,errMsg)
  }
  
  private def removeBom(word:String)={
    if(!word.isEmpty() && "\uFEFF".charAt(0)==word.charAt(0))word.substring(1) else word  
  }
  
  private def getCountryCode(bic:String):Option[String]=if(bic.length>=6)Some(bic.substring(4, 6)) else None
  
  private def getOwnersBic(direction:String,bicBlocks:Map[String,String]):Option[String]={
      if(bicBlocks.contains("1") && bicBlocks.contains("2")){    
        val identifier=if(bicBlocks("2").length >=16){
          bicBlocks("2").substring(0, 1).toUpperCase
        }else ""
          
        (direction.toUpperCase,identifier) match {//bic_8
          case ("S","I") |("R","O") =>{
            if(bicBlocks("1").length>=15)
              Some(bicBlocks("1").substring(3, 11)) //logical terminal address
            else None  
          }          
          case ("S","O") | ("R","I")=>{
            Some(bicBlocks("2").substring(4, 12)) //correspondent address
          }
          case (_,_) =>None
        }       
      }else None
  }  

 /**
 * @param message Multi-line for only one complete message that contains header stored in Array
 * @return  Tuple of (correspondent,country, messageType,mtPSV,blockFieldPSV,auditPSV,irn)
 * @throws Throws [[main.scala.MTMessage.MTParserException]], be careful.
 * {{{
 *  val message=Array("129Aug2009     SGC/MRMDUS33    /S/MT103S/92410000273 /SGN2407964M2PL8G/000000/000000/USD/7000.00             /HSBCSGSG     Page:   1",
 *  		"{1:F01MIDLGB20AXXX0000000000}",
 *  	  "2014-07-01-14.20.41     CBL       MQS Link 1122 Received message: from Application assigned to CBL using LKD Rule default")
 *  
 * getPSVContents(message) match{
 *     case Success((correspondent, country, messageType, mtMessagePSV, blockFieldMessagePSV, messageAuditPSV,irn)) => {
 *         (correspondent, country, messageType, mtMessagePSV, blockFieldMessagePSV, messageAuditPSV,irn)
 *     }
 *     case Failure(ex) => {
 *         ex match{
 *            case ex:MTParserException=> throw ex 
 *         }   
 *       }
 *     }
 * }}}
 */    
  @throws(classOf[MTParserException])  
  def getPSVContents(message: Array[String]):Try[(String,String,String,String,String,String,String)]= {
    Try({
      val (
        recordDate, department, correspondent, direction,
        messageType, irn, trn, isn, osn, ccy, amount, senderBic
      ) = if(message.length >= 1 && !message(0).isEmpty())headerItems(message(0)) else ("","","","","","","","","","","","")  
      
      if (message.length > 1 && !message(0).isEmpty()) {      
        val blockFieldMessagePSV = ArrayBuffer[String]()
        val messageAuditPSV = ArrayBuffer[String]();    
        val (asisMessage, blockFieldValues, audits) = messageItems(message)
        if(!asisMessage.isEmpty){
          val uuid = java.util.UUID.randomUUID.toString
          val bicBlocks=blockFieldValues.filter(i=>{
               (i._1=="1"||i._1=="2") && (i._2.trim.isEmpty) && !(i._3.trim.isEmpty)
          }).map{i=>{(i._1,i._3)}}.toMap           
          
          val ownersBic=if(!senderBic.isEmpty)senderBic.substring(0,8) else getOwnersBic(direction,bicBlocks).getOrElse("") 
          val country_code=getCountryCode(ownersBic).getOrElse("")

          blockFieldValues.foreach(i => {
            blockFieldMessagePSV += (uuid + "|" + recordDate + "|" + region+"|"+
                messageType + "|" +ownersBic + "|" + correspondent + "|" + 
                direction + "|" +department + "|" + irn + "|" + trn + "|" + isn + "|" +
                osn + "|" + ccy + "|" + amount + "|" +senderBic + "|" + 
                i._1 + "|" + i._2 + "|" + i._3 + "|" + i._4 +"\n")
          })
          audits.foreach(i => {
            messageAuditPSV += (uuid + "|" + recordDate + "|" + region+"|"+ 
                               messageType + "|" +ownersBic+"|"+ 
                               i._1 + "|" + i._2 + "|" + i._3 + "\n")
          })
    
          val mtMessagePSV = (
              uuid + "|" + recordDate + "|" + region+"|"+
              messageType + "|" +ownersBic+"|"+ 
              encode(asisMessage) + "\n"
          )
      
          (ownersBic, country_code, messageType, mtMessagePSV.trim, blockFieldMessagePSV.mkString.trim, messageAuditPSV.mkString.trim,irn)        
        }else{ 
          val errMsg="IRN(%s) MESSAGE(%s)".format(irn.toString,messageType)          
          //logger.debug("BODY[{}]",message.mkString("\n"))  
          addToBodyStats(messageType)                    
          throw MTParserException(Error.INVALID_BODY,errMsg)
        }
      }
      else {
        val errMsg="IRN(%s) MESSAGE(%s)".format(irn.toString,messageType) 
        //logger.debug("HEADER[{}]",message.mkString) 
        addToHeaderStats(messageType)        
        throw MTParserException(Error.INVALID_HEADER,errMsg)
      }
    })
  }

 /**
 * @param message Multi-line for only one complete message that contains header stored in Array
 * @return  Tuple of Future(bic,country, messageType,mtPSV,blockFieldPSV,auditPSV,irn)
 * @throws Throws [[main.scala.MTMessage.MTParserException]], be careful.
 * {{{
 *  val message=Array("129Aug2009     SGC/MRMDUS33    /S/MT103S/92410000273 /SGN2407964M2PL8G/000000/000000/USD/7000.00             /HSBCSGSG     Page:   1",
 *  		"{1:F01MIDLGB20AXXX0000000000}",
 *  	  "2014-07-01-14.20.41     CBL       MQS Link 1122 Received message: from Application assigned to CBL using LKD Rule default")
 *  
 * getFuturePSVContents(message).onComplete({
 *     case Success((bic, country, messageType, mtMessagePSV, blockFieldMessagePSV, messageAuditPSV,irn)) => {
 *         (correspondent, country, messageType, mtMessagePSV, blockFieldMessagePSV, messageAuditPSV,irn)
 *     }
 *     case Failure(ex) => {
 *         ex match{
 *            case ex:MTParserException=> throw ex
 *         }   
 *       }
 *   })
 * }}}
 */    
  @throws(classOf[MTParserException])  
  def getFuturePSVContents(message: Array[String])= future{
    getPSVContents(message) match{
       case Success((bic, country, messageType, mtMessagePSV, blockFieldMessagePSV, messageAuditPSV,irn)) => {
         (bic, country, messageType, mtMessagePSV, blockFieldMessagePSV, messageAuditPSV,irn)
       }
       case Failure(ex) => {
         ex match{
           case ex:MTParserException=> throw ex
         }   
       }
    }    
  }  
  
  try{     
    
    if(ondemandFile.exists && ondemandFile.length*2 >=ondemandFile.getUsableSpace){
      val errMsg="FREE SPACE(%d)".format(ondemandFile.getUsableSpace)
      throw MTParserException(Error.INSUFFICIENT_DISK_SPACE,errMsg)
    }      
     
    logger.info("[{}({})][FILE SIZE][{}]",ondemandFile.getName,region,ondemandFile.length.toString)
    logger.info("[{}({})][FREE SPACE][{}]",ondemandFile.getName,region,ondemandFile.getUsableSpace.toString)    
    
    //input files
    inputFiles+=("bic"->Some(Source.fromFile(File.separator+bicFilename)("UTF-8")))
    inputFiles+=("country"->Some(Source.fromFile(File.separator+countryFilename)("UTF-8")))
    inputFiles+=("scope"->Some(Source.fromFile(File.separator+scopeFilename)("UTF-8")))
    inputFiles+=("region"->Some(Source.fromFile(File.separator+regionsFilename)("UTF-8")))
    inputFiles+=("ondemand"->Some(Source.fromFile(ondemandFile, bufferSize)))       
    
    //output file names
    outputFileNames+=("dtcs_message"->(outputDir+prefix+"message"))
    outputFileNames+=("dtcs_audit"->(outputDir + prefix+"audit"))
    outputFileNames+=("dtcs_blockfield"->(outputDir + prefix+"blockfield"))
    outputFileNames+=("non_dtcs_message"->(outputFileNames("dtcs_message") +suffix))
    outputFileNames+=("non_dtcs_audit"->(outputFileNames("dtcs_audit") +suffix))
    outputFileNames+=("non_dtcs_blockfield"->(outputFileNames("dtcs_blockfield") +suffix))  

    //DTCS Files
    outputFiles+=("dtcs_message"->Some(new PrintWriter(new BufferedWriter(new FileWriter(outputFileNames("dtcs_message")), bufferSize))))
    outputFiles+=("dtcs_audit"->Some(new PrintWriter(new BufferedWriter(new FileWriter(outputFileNames("dtcs_audit")), bufferSize))))
    outputFiles+=("dtcs_blockfield"->Some(new PrintWriter(new BufferedWriter(new FileWriter(outputFileNames("dtcs_blockfield")), bufferSize))))
    
    //NON DTCS Excluded Files
    outputFiles+=("non_dtcs_message"->Some(new PrintWriter(new BufferedWriter(new FileWriter(outputFileNames("non_dtcs_message")), bufferSize))))
    outputFiles+=("non_dtcs_audit"->Some(new PrintWriter(new BufferedWriter(new FileWriter(outputFileNames("non_dtcs_audit")), bufferSize))))
    outputFiles+=("non_dtcs_blockfield"->Some(new PrintWriter(new BufferedWriter(new FileWriter(outputFileNames("non_dtcs_blockfield")), bufferSize))))  
    
    val whiteListedBics = inputFiles("bic").get.getLines.filter {reject}.map(x=>x.trim.toUpperCase).toArray
    val excludedCountries = inputFiles("country").get.getLines.filter {reject}.map(x=>x.trim.toUpperCase).toArray
    val  mtScopes = inputFiles("scope").get.getLines.filter {reject}.map(x=>x.trim.toUpperCase).toArray
    val regions = inputFiles("region").get.getLines.filter {reject}.map(x=>x.trim.toUpperCase).toArray      
  
    if(!isRegionValid(region)){
      val errMsg="REGION(%s)".format(region)
      throw MTParserException(Error.INVALID_REGION,errMsg)      
    }
    
    def isRegionValid(region: String) = regions.contains(region) 
  
    def isMessageInScope(messageType: String) = mtScopes.contains(messageType.toUpperCase)
    
    def matchExclusionCriteria(bic: String, countryCode: String) = {
      if (whiteListedBics.contains(bic.toUpperCase)) false
      else if (excludedCountries.contains(countryCode.toUpperCase)) true
      else false
    } 
   
    def  fail(lineNos:Int,ex:MTParserException)={ 
     val errMsg="LINE(%s) %s".format(lineNos.toString,ex.getMessage) 
     logError(ondemandFile.getName,region,ex.errorType.toString,errMsg)            
    } 
    
    def success(bic:String, country:String,messageType:String, 
                 mtMessagePSV:String, blockFieldMessagePSV:String,messageAuditPSV:String,irn:String)={
        val isDTCS = {
          matchExclusionCriteria(bic, country) == false
        }
        val inScope = isMessageInScope(messageType)  
        
        //logger.debug("SCOPE[{}({})][{}][{}][{}]",ondemandFile.getName,region,irn,messageType,inScope.toString.toUpperCase);     
        //logger.debug("DTCS[{}({})][{}][{}][{}][{}]",ondemandFile.getName,region,irn,bic,country,isDTCS.toString.toUpperCase);  

        (inScope, isDTCS) match {
          case (true, true) => {
            dtcsCount+=1
            scopeCount+=1
            addToMessageStats(messageType.toUpperCase)
            addToCountryStats(country.toUpperCase)
            addToBicStats(bic.toUpperCase)
            
            if (!mtMessagePSV.isEmpty &&  outputFiles.contains("dtcs_message")) outputFiles("dtcs_message").get.println(mtMessagePSV)
            if (!messageAuditPSV.isEmpty &&  outputFiles.contains("dtcs_audit")) outputFiles("dtcs_audit").get.println(messageAuditPSV)
            if (!blockFieldMessagePSV.isEmpty &&  outputFiles.contains("dtcs_blockfield")) outputFiles("dtcs_blockfield").get.println(blockFieldMessagePSV)
          }
          case (true, false) => {
            scopeCount+=1
            if (!mtMessagePSV.isEmpty && outputFiles.contains("non_dtcs_message")) outputFiles("non_dtcs_message").get.println(mtMessagePSV)
            if (!messageAuditPSV.isEmpty && outputFiles.contains("non_dtcs_audit")) outputFiles("non_dtcs_audit").get.println(messageAuditPSV)
            if (!blockFieldMessagePSV.isEmpty && outputFiles.contains("non_dtcs_blockfield")) outputFiles("non_dtcs_blockfield").get.println(blockFieldMessagePSV)
          }
          case (false, true) => {
            dtcsCount+=1
          }case(_,_) =>{
            //Log Info
          }
        }    
    }

    for {
        lines<- inputFiles("ondemand").get.getLines()
     } {
      lineIndex+=1 
      val line=removeBom(lines)
      if (isMessageHeader(line) && message.length == 0) {//first message item
        message += line
      } else if (isMessageHeader(line) && message.length > 0) {//next message item
        totalCount+=1
        getFuturePSVContents(message.toArray).onComplete({
           case Success((bic, country, messageType, mtMessagePSV, blockFieldMessagePSV, messageAuditPSV,irn)) => {             
             synchronized {
               success(bic, country, messageType, mtMessagePSV, blockFieldMessagePSV, messageAuditPSV,irn)
             }
           }
           case Failure(ex) => {
             synchronized {
               ex match{
                 case ex:MTParserException=> fail(lineIndex-message.size,ex) 
               }
             }
           }
        })
        message.clear
        message += line
      } else if (message.length > 0) { //message continues on next line
        message += line
      }
    }
      
    if (message.length > 0) {// last message item.
      lineIndex+=1 
      totalCount+=1
      getFuturePSVContents(message.toArray).onComplete({
         case Success((bic, country, messageType, mtMessagePSV, blockFieldMessagePSV, messageAuditPSV,irn)) => {           
           synchronized {
             success(bic, country, messageType, mtMessagePSV, blockFieldMessagePSV, messageAuditPSV,irn)
           }
         }
         case Failure(ex) =>{
           synchronized {
             ex match{
               case ex:MTParserException=> fail(lineIndex-message.size,ex) 
             }
           }            
         }
      })
      message.clear
    }
    
    while(thread.getCompletedTaskCount !=thread.getTaskCount){}
    if(0==totalCount){
      val errMsg="TOTAL TASKS(%d)".format(totalCount)
      throw MTParserException(Error.INVALID_FILE,errMsg)         
    }    
    
    
    logger.info("[{}({})][TOTAL THREADS][{}]",ondemandFile.getName,region,numWorkers.toString)    
    logger.info("[{}({})][TOTAL TASKS][{}]",ondemandFile.getName,region,thread.getCompletedTaskCount.toString)        
    
    logger.info("[{}({})][TOTAL MESSAGES][{}]",ondemandFile.getName,region,totalCount.toString)
    logger.info("[{}({})][SUCCESS MESSAGES][{}]",ondemandFile.getName,region,(totalCount-failedHeaderCount-failedBodyCount).toString)
    logger.info("[{}({})][FAILED HEADER MESSAGES][{}]",ondemandFile.getName,region,failedHeaderCount.toString)
    logger.info("[{}({})][FAILED BODY MESSAGES][{}]",ondemandFile.getName,region,failedBodyCount.toString) 
    
    logger.info("[{}({})][SCOPE MESSAGES][{}]",ondemandFile.getName,region,scopeCount.toString)
    logger.info("[{}({})][DTCS MESSAGES][{}]",ondemandFile.getName,region,dtcsCount.toString)
        
    closeOutFiles
    
    //Stats
    val statsFile=new PrintWriter(new BufferedWriter(new FileWriter(outputDir + prefix+"stats"), bufferSize))
    statsFile.println(getStatsPSV)
    statsFile.flush
    statsFile.close    
    logger.debug("FILE[{}({})][CLOSED]","stats",region)
  }catch {
    case ex:MTParserException =>{
      logError(ondemandFile.getName,region,ex.errorType.toString,ex.getMessage) 
      exitStatus=1
    }
    case ex:Exception => {
      logError(ondemandFile.getName,region,ex.getClass.toString,ex.getMessage) 
      exitStatus=1
    }
  }finally{//close all open files in case of exception
      closeInFiles     
      ec.shutdown()  
      logger.debug("EXIT[STATUS][{}]",exitStatus.toString)
      
			val finishDate=new Date();
			val units=TimeUnit.MILLISECONDS;   
			val duration=getDateDiff(startDate,finishDate,units);		
			logger.info("[{}({})][TOTAL DURATION(ms)][{}]",ondemandFile.getName,region,duration.toString);	      
      System.exit(exitStatus); 
  }
}
