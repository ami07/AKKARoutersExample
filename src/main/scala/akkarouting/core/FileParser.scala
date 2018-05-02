package akkarouting.core

object FileParser {

  def parse(streamLine : String) : (String, List[String])={
    val parsedInsertionTuple: Array[String] = streamLine.split('|')
    val firstLineSplit = parsedInsertionTuple.head.stripPrefix("+")
    val relationName=firstLineSplit.map(n => if(n.isLetter) n else ' ').trim
    val firstColumn = firstLineSplit.map(n => if(n.isLetter) ' ' else n).trim
    val tuple = List(firstColumn) ++ parsedInsertionTuple.tail.toList
    (relationName, tuple)
  }

}
