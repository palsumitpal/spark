import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import collection.mutable.HashMap
import collection.mutable.HashSet
import collection.mutable.LinkedHashSet
import collection.mutable.ArrayBuffer

// ASSUME -- 1st line is header
// ASSUME -- Field separator is '\t'
// ASSUME -- no field contains '\t'
// ASSUME -- output binarized filename == inputfilename_binarized

// Arguments	-- 1st Arg - name of the input file
//				-- 2nd Arg - the column Numbers (starting from 0 as start Index) in the input file which needs to be binarized

object BinarizeAllColumns
{
	def main(args: Array[String]) 
	{
		val sc = new SparkContext("local", "Binarize", System.getenv("SPARK_HOME"))
		val fileToBinarize = args(0)

		val lines = sc.textFile(fileToBinarize)
		val headers = lines.first.split("\t")
		
		// filter out the 1st line - which is the header
		val items = lines.filter(x => ! x.contains(headers(0))).map( x => x.split("\t"))
		val numberOfColumnsInFile = items.first.length
		
		val cols = args.tail.length
		
		// distincts in each column
		val distincts = new Array[collection.mutable.ArrayBuffer[String]](cols)

		for(j <- 0 to cols-1)
			distincts(j) = new collection.mutable.ArrayBuffer[String]()
		
		var j = 0
		var binarized = scala.collection.mutable.Set[Int]()
		for(a <- args.tail){
			val d = items.map(x => x(a.toInt)).distinct.toArray
			distincts(j) ++= d
			j = j + 1
			binarized = binarized + a.toInt
		}
		
		val global = (0 to numberOfColumnsInFile-1) toSet
		val notBinarized = global -- binarized.toSet
		
		val columnDistincts = sc.broadcast(distincts)
		val columnsToBeBinarized = sc.broadcast(args.tail)
		val columnsNotToBeBinarized = sc.broadcast(notBinarized)
		
		val binarize = items.map( x => 
		{
			var binaryLine = ""
			var a = 0
			for(j <- columnsToBeBinarized.value)
			{
				val setSize = columnDistincts.value(a).length
				val indexCh = columnDistincts.value(a).indexOf(x(j.toInt))
				var s1 = for(z <- 0 to (indexCh-1)) yield "0"
				var s2 = for(z <- (indexCh+1) to (setSize-1) ) yield "0"
				var s3 = if(s1.length == 0) "1 " else { if (s2.length == 0) " 1"  else " 1 "}
				var s4 = s1.mkString(" ") + s3 + s2.mkString(" ")				
				binaryLine += s4 + " "
				a = a + 1
			}
			
			// non binary columns are at the end
			var nonBinary = ""
			for(j <- columnsNotToBeBinarized.value)
				nonBinary += x(j.toInt) + " "
			
 			binaryLine.trim + " " + nonBinary.trim
		}
		)
		var s = ""
		
		// generate headers - if there are no headers for the original file
		for( a <- 1 to cols)
		{
			val c = args(a).toInt  // Column Number
			val h = headers(c) // header
			var t = ""
			for( x <- distincts(a-1))
				t = t.concat(h).concat("_").concat(x).concat(" ")
			s = s.concat(t).concat(" ")
		}
		for(a <- notBinarized )
			s = s.concat(headers(a)).concat(" ")
			
		// Print header to console followed by the binarized values
		println(s.trim)
		binarize foreach println
		
		// write to an output file without headers
		binarize.saveAsTextFile("binarizedout/" + "Binarized.txt")
	}
}
