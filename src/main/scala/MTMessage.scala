package main.scala

import scala.collection.mutable.ArrayBuffer
import com.typesafe.scalalogging.LazyLogging

/**
 * @author Terry Adeagbo
* @note The implemented interfaces are used to parse the message '''Header''','''Body''' and '''Audit'''
* @version 1.0
*/

trait MTMessage extends LazyLogging{ 
  
  
 /**
 * @note Indicates if logging is turned on or off"  
 */   
  protected var isLoggingEnabled=false

 /**
 * @note Enums for indicating the Error type when parsing on demand file  
 */   
  protected object Error extends Enumeration {
    type Error =Value
    val INVALID_HEADER,
        INVALID_BODY,
        INVALID_FILE,
        INVALID_REGION,
        INSUFFICIENT_DISK_SPACE= Value
  }   
  
/**
 * @note Message header starts with "1" 
 */
  protected val headerStartText="1"
  
/**
 * @note Message header ends with "Page:   1" 
 */  
  protected val headerEndText="Page:   1"

 /**
 * @note Message header has a fixed length of "132 characters"  
 */  
  protected val headerLength=132
  
 /**
 * @note Message Audit has a minimum length of "35 characters"  
 */  
  protected val auditMinLength=35

 /**
 * @note Maximum allowed records in auditItems  
 */    
  protected val auditMaxRecordLength=2000 
  
/**
 * @note Regular expression to identify message audit line
 * 
 * {{{
 * "^[\\s]*[\\d]{4}[-][\\d]{2}[-][\\d]{2}[-][\\d]{2}[\\.][\\d]{2}[\\.][\\d]{2}[\\s]{5}.*"
 * }}}
 */  
  protected val auditPattern="^[\\s]*[\\d]{4}[-][\\d]{2}[-][\\d]{2}[-][\\d]{2}[\\.][\\d]{2}[\\.][\\d]{2}[\\s]{5}.*".r
  
  
 /**
 * @note Enums for indicating the current parser state in the tagger method  
 */   
  private object State extends Enumeration {
    type State =Value
    val BEFORE_START_OF_BLOCK,
        IN_BLOCK_KEY,
        IN_LINE,
        IN_FIELD_KEY,
        IN_FIELD_VALUE,
        IN_VALUE = Value
  } 
  
 /**
 * @constructor Create an MT Parser Exception with an errorType,irn,message type and actual error message
 * @param errorType enum value that stores the type of [[main.scala.MTMessage.Error]]  
 * @param msg contains message text    
 * {{{
 *  val exception= MTParserException(Error.INVALID_BODY,"IRN[142120000026] MESSAGE[MT103] BODY[1:F01MIDLGB20AXXX0000000000]")
 * }}} 
 */    
  case class MTParserException (errorType:Error.Value,msg:String) extends Exception(msg)
  
/**
 * @param line String being tested to contain message header
 * @return '''True''' if line contains message header '''False''' otherwise 
 * {{{
 *  val header="129Aug2009     SGC/MRMDUS33    /S/MT103S/92410000273 /SGN2407964M2PL8G/000000/000000/USD/7000.00             /HSBCSGSG     Page:   1"
 *  val result=isMessageHeader(header)
 *  Returns True
 * }}}
 */ 
 def isMessageHeader(line:String)={
     line.trim.length()==headerLength && 
     line.trim.startsWith(headerStartText) && 
     line.endsWith(headerEndText)
 }

/**
 * @param line String being tested to contain message audit 
 * @return '''True''' if line matches audit pattern '''False''' otherwise 
 * {{{
 *  val audit="2014-07-01-14.20.41     CBL       MQS Link 1122 Received message: from Application assigned to CBL using LKD Rule default"
 *  val result=isMessageAudit(audit)
 *  Returns True
 * }}}
 */   
  def isMessageAudit(line:String)=auditPattern.findFirstIn(line)!=None &&  line.trim.length()>=auditMinLength

/**
 * @param header String that contains message header
 * @note Header string must be a valid message header text 
 * @return Tuple(recordDate,department,correspondent,direction,messageType,irn,trn,isn,osn,ccy,amount,senderBic)
 * @throws Throws [[main.scala.MTMessage.MTParserException]] for invalid header, be careful.
 * {{{
 *  val header="129Aug2009     SGC/MRMDUS33    /S/MT103S/92410000273 /SGN2407964M2PL8G/000000/000000/USD/7000.00             /HSBCSGSG     Page:   1"
 *  val result=headerItems(header)
 *  Returns ("29AUG2009","SGC","MRMDUS33","S","MT103S","92410000273","SGN2407964M2PL8G","000000","000000","USD","7000.00","HSBCSGSG")
 * }}}
 */ 
  @throws(classOf[MTParserException])  
  def headerItems(header:String)={
    if(!isMessageHeader(header)) throw MTParserException(Error.INVALID_HEADER,"HEADER["+ header+"]")
    
    val recordDate    = header.substring(1, 15).trim
    val department    = header.substring(15, 18).trim
    val correspondent = header.substring(19, 31).trim
    val direction     = header.substring(32, 33).trim
    val messageType   = header.substring(34, 40).trim
    val irn           = header.substring(41, 53).trim
    val trn           = header.substring(54, 70).trim
    val isn           = header.substring(71, 77).trim
    val osn           = header.substring(78, 84).trim
    val ccy           = header.substring(85, 88).trim
    val amount        = header.substring(89, 109).trim
    val senderBic     = header.substring(110, 123).trim
//    val country       = if(correspondent.length() < 6) "" else correspondent.substring(4, 6).trim    
    (recordDate,department,correspondent,direction,messageType,irn,trn,isn,osn,ccy,amount,senderBic)
  }

/**
 * @param audit Multi-line audits stored in Array 
 * @return Array of Tuple(timestamp,code,messageText)
 * @note Audit string must be a valid message audit text<br>
 * Code is one of '''CBL''', '''SYS''', '''MQS'''<br>
 * {{{
 *  val audit=Array("2014-07-01-14.20.41     CBL       MQS Link 1122 Received message: from Application assigned to CBL using LKD Rule default")
 *  val result=auditItems(audit)
 *  Returns Array(("2014-07-01-14.20.41","CBL","MQS Link 1122 Received message: from Application assigned to CBL using LKD Rule default"))
 * }}}
 */    
  def auditItems(audit:Array[String])={
   val multiLines = ArrayBuffer[String]()
   var items=ArrayBuffer[String]()    
   
   audit.foreach { x => {
      if(isMessageAudit(x) && multiLines.length==0){//first audit item
          multiLines+=x
      }else if(!isMessageAudit(x) && multiLines.length>0){//audit continues on next line
          multiLines+=x
      }else if(isMessageAudit(x) && multiLines.length>0){//next audit item
          items+=(multiLines.mkString)
          multiLines.clear    
          multiLines+=x
      }
   }}
   
   if(multiLines.length>0){//last audit item
     items+=(multiLines.mkString)
     multiLines.clear        
   }
   
   val maxLength=if(items.length<auditMaxRecordLength){
     items.length 
   }else {
     if(isLoggingEnabled){
       logger.warn("Audit items record "+items.length+" is greater than maximum audits record "+auditMaxRecordLength)       
     } 
     auditMaxRecordLength
   }
   
   items=for{
     (item,index)<-items.zipWithIndex
     if(index<maxLength)
   }yield item
 
   items.map{ x =>{
       val line=" "+x.replace("\n", "").replace("\r", "").trim  
       val timestamp=line.substring(1,25).trim 
       val code=line.substring(25,35).trim 
       val messageText=line.substring(35).trim()
       (timestamp,code,messageText)               
     }  
   }.toArray
  }
  
/**
 * @param message Multi-line contain at least one message body stored in Array
 * @return Original Swift MT message and Array of Tuple(level,tag,value) 
 * @note Level shows the number of nested blocks <br>
 * Tag is one of 'block', 'field', 'value'<br>
 * Value must be part of the Swift X sets valid characters<br>
 * '''a b c d e f g h i j k l m n o p q r s t u v w x y z<br>
 * A B C D E F G H I J K L M N O P Q R S T U V W X Y Z<br>
 * 0 1 2 3 4 5 6 7 8 9<br>
 * / - ? : ( ) . , ’ + { }<br>
 * CR LF Space<br>'''
 * Message string must be a valid swift message text enclosed in open and close curly braces
 * {{{
 * val message =Array("{1:F01MIDLGB20AXXX0000000000}","{2:I103BMUSOMRXXXXXN}")
 * val (mtMessage,tags)=tagger(message)
 * Returns mtMessage:  "{1:F01MIDLGB20AXXX0000000000}{2:I103BMUSOMRXXXXXN}" 
 *       tags: Array((1,"block","1"),(1,"value","F01MIDLGB20AXXX0000000000"),(1,"block","2"),(1,"value","I103BMUSOMRXXXXXN"))
 * }}}
 */    
  def tagger(message:Array[String]):(String,Array[(Int,String,String)]) = {
    var state =State.BEFORE_START_OF_BLOCK    
    var blockKey = ""
    var fieldKey = ""
    var level = 0
    val list = ArrayBuffer[(Int,String,String)]()
    val mtMessage=new StringBuilder
    val text = new StringBuilder
    
    for {//filter next page header from message
      line<-message       
      c <- (line+"\n")  
      if((line.startsWith(headerStartText) && line.length()==headerLength)==false)
      if(isMessageAudit(line)==false)  
    } {
      state match {
        case State.BEFORE_START_OF_BLOCK =>{
          if (c == '{'){//_InBlockKey
            mtMessage+=c
            state = State.IN_BLOCK_KEY
            level += 1
            text.clear
          }else if(c=='}'){
            mtMessage+=c
            if(level==0){
              if(isLoggingEnabled)logger.warn("unmatched {} outside of any block must be removed",c.toString)
            }else{
              level -= 1
            }
          }else if (c == '\n'){
            mtMessage+=c
            if(level!=0){
              if(isLoggingEnabled)logger.warn("nested block must be closed (state={}, level={})",state.toString,level.toString)
            }else  {//TO DO:
              //list+=((level, "message", ""))
            }
          }else if (c != '\r'){//TO DO:
            //logger.info("block must start with { instead of {}",c.toString)
          }
        }

        case State.IN_BLOCK_KEY=>{
          mtMessage+=c
          if (c == ':'){ //InLine
            state = State.IN_LINE
            blockKey =text.toString
            list+=((level, "block", blockKey))
            text.clear
          }else if (c.isLetterOrDigit){
            text += c
          }else{
            if(isLoggingEnabled)logger.warn("block id must consist of letter or digit bug encountered {}",c.toString)
          }
        }

        case State.IN_LINE=>{
          mtMessage+=c
          if (c == '{'){//_InBlockKey
            state = State.IN_BLOCK_KEY
            level += 1
            text.clear
          }else if (c == '}'){//BeforeStartOfBlock
            state = State.BEFORE_START_OF_BLOCK
            level -= 1
            //assert level >= 0
          }else if (c == ':' ){//InFieldKey
            state =State.IN_FIELD_KEY
            text.clear
          }else if (c!=' ' && c!='\t' && c != '\n'){//InValue
            state = State.IN_VALUE
            text.clear
            text += c
          }
        }

        case State.IN_FIELD_KEY=>{
          mtMessage+=c
          if (c == ':' ){//InFieldValue
            state = State.IN_FIELD_VALUE
            fieldKey = text.toString
            text.clear
          }else if (c.isLetterOrDigit){
            text += c
          }
        }

        case State.IN_FIELD_VALUE=>{
          mtMessage+=c
          if (c == '\n' || c == '}' ||c== '\r' ){
            list+=((level, "field", fieldKey))
            list+=((level, "value", text.toString))              
            fieldKey = ""
            text.clear
            if (c == '}'){//BeforeStartOfBlock
              state =State.BEFORE_START_OF_BLOCK
              level -= 1
            } else{//InLine
              state =State.IN_LINE
            }
          }else{
            text += c
          }
        }

        case State.IN_VALUE=>{
          mtMessage+=c
          if (c == '\n' || c == '}' ||c== '\r'){
            list+=((level, "value", text.toString))
            text.clear
            if (c == '}'){//BeforeStartOfBlock
              state =State.BEFORE_START_OF_BLOCK
              level -= 1
              //assert level >= 0
            } else{//InLine
              state =State.IN_LINE
            }
          }else{
            text += c
          }
        }
        case _=>{
          if(isLoggingEnabled)logger.warn("Invalid state reached: {}",state.toString)
        }
      }
    }

    (mtMessage.toString.trim,list.toArray)
  }  

 /**
 * @param tags Array of tags identified in a message body
 * @return  Array of Tuple(block,field,value,line_order)
 * {{{
 * val tags =Array((1,"block","1"),(1,"value","F01MIDLGB20AXXX0000000000"),(1,"block","2"),(1,"value","I103BMUSOMRXXXXXN"))
 * val items=blockFieldItems(tags)
 * Returns Array(("1","","F01MIDLGB20AXXX0000000000",1),("2","","I103BMUSOMRXXXXXN",2))
 * }}}
 */
 def blockFieldItems(tags:Array[(Int,String,String)]):Array[(String,String,String,Int)]={
      var block = ""
      var childBlock = ""
      var field = ""
      val valueSoFar = ArrayBuffer[String]()
      val multiLines = ArrayBuffer[String]()
      val array = ArrayBuffer[(String,String,String)]()      
      val addItem=()=>{
        if(multiLines.length==1){
          if(!childBlock.trim.isEmpty) {
            array += ((block, childBlock, valueSoFar.mkString))
          } else {
            array += ((block, field, valueSoFar.mkString))
          }
        }else{
          if(!field.trim.isEmpty()){
            array += ((block, field, multiLines.mkString(";")))
          }else{
            array += ((block, childBlock, multiLines.mkString(";")))
          }
        }         
      }
      
      for {
        (level,kind,value)  <-tags  
       }{
         kind match {
           case "block"=>{
               if (level == 1) {
                  if(!block.trim.isEmpty ) {
                    addItem()
                  }
                  block = value
                  childBlock = ""
                  field=""
                  valueSoFar.clear
                  multiLines.clear
              } else if(level == 2){
                if(!childBlock.trim.isEmpty ) {
                  if(multiLines.length==1){
                    array += ((block, childBlock, valueSoFar.mkString))
                  }else{
                    array += ((block, childBlock, multiLines.mkString(";")))
                  }
                }
                childBlock = value
                field = ""
                valueSoFar.clear
                multiLines.clear
              }  else {
                field = value
              }            
           }
           case "field"=>{            
              if(!field.trim.isEmpty && !valueSoFar.isEmpty) {
                  if(multiLines.length==1){
                    array += ((block, field, valueSoFar.mkString))
                  }else{
                    array += ((block, field, multiLines.mkString(";")))
                  }
              }else{
                  if(!valueSoFar.isEmpty)array += ((block, field, valueSoFar.mkString))
              }
              valueSoFar.clear
              multiLines.clear              
              field=value
           }
           case "value"=>{
              val new_value=if(value.contains(';'))value.replaceAll(";", "") else value          
              multiLines+=new_value
              valueSoFar+=new_value         
           }
           case _=>{
             if(isLoggingEnabled)logger.warn("Invalid tag found : {}",kind)
           }           
         }
     }
     // add the last item.
     addItem()
     for ((list,count)<- array.toArray.zipWithIndex)yield {
       (list._1,list._2,list._3,count+1)
     }
   }   
    
 /**
 * @param message Multi-line contain at least one message body and audits stored in Array
 * @return  Tuple of (Original Message,Message(Block,Field,Value,LineOrder),Audit(Timestamp,Code,MessageText))
 * {{{
 *  val message=Array("{1:F01MIDLGB20AXXX0000000000}","{2:I103BMUSOMRXXXXXN}",
 *  "2014-07-01-14.20.41     CBL       MQS Link 1122 Received message: from Application assigned to CBL using LKD Rule default")
 *  
 *  val (mtMessage,blockFieldItems,audits)=messageItems(message)
 *  Returns mtMessage:  "{1:F01MIDLGB20AXXX0000000000}{2:I103BMUSOMRXXXXXN}" 
 *   	blockFieldItems: Array(("1","","F01MIDLGB20AXXX0000000000",1),("2","","I103BMUSOMRXXXXXN",2))
 *  	audits:  Array(("2014-07-01-14.20.41","CBL","MQS Link 1122 Received message: from Application assigned to CBL using LKD Rule default"))
 * }}}
 */   
 def messageItems(message:Array[String]):(String,Array[(String,String,String,Int)],Array[(String,String,String)])={
   val tags=tagger(message) 
   val mtMessage=tags._1
   val blockFieldValues=blockFieldItems(tags._2)
   val bodyLength=mtMessage.split("\n").length   
   val items=message.toBuffer   
   val audits=if(bodyLength<items.length){
     items.remove(0,bodyLength)   
     auditItems(items.toArray)     
   }else {
     if(isLoggingEnabled)logger.warn("No audit items found: {}",message.mkString("\n"))
     Array[(String,String,String)]()
   }
   (mtMessage,blockFieldValues,audits)
  }
}
