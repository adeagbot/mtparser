package main.java;

/**
 * @author Terry Adeagbo
 * @note The entry thread into the Application.
 * @version 1.0
 */

public class Runner { 
  
  public static void main(String args[]){
	 if(args.length!=8){
		    System.err.println("Incorrect number of arguments passed: "+args.length);
		    System.out.println("Arguments expected: java -jar mtparser.jar"
		    		+ " [Instance<UK,US,AP>] "
		    		+ " [on demand file] "
		    		+ " [output directory] "
		    		+ " [log directory] "
		    		+ " [bics file] " 
		    		+ " [country file] "
		    		+ " [scope file] "
		    		+ " [region file]");
		    System.exit(1);     
	 }	  
	 System.setProperty("log.dir",args[3]);
	 main.scala.MTParserApp.main(args);
  }
}
