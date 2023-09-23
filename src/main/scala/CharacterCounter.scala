
import scala.collection.immutable.{Map, _}
import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorSystem

/*
  Spawn actor class

  Receives list of strings and based the size of the list,
  it determines how many count actors it needs to spawn and allocates a
  part of the list to the each of them

 */
class SpawnActor extends Actor{

  var charCount : Map[Char, Int] = Map()
  var awaitingCount : Int = 0

  // Handles messages
  def receive: Receive = {
    case s: List[String] => determineHowManyActors(s)
    case s: Map[Char, Int] => combineCounts(s)
  }

  /** Determines how many count actors to spawn based on list size and
   *  spawns them while allocating each a part of the list
   *
   *  @param list file as list
   */
  def determineHowManyActors(list : List[String]): Unit = {

    // split list by lines of 10
    val splitList = list.grouped(20).toList
    awaitingCount = splitList.size

    // Spawn actors
    println("Created " + splitList.size + " count actors")
    spawnActors(splitList, splitList.size)

  }

  /** Spawns count actors and sends a list for them to process
   *
   *  @param list list which the actor will process
   *  @param x how many more actors to spawn
   */
  def spawnActors(list : List[List[String]], x: Int): Unit = {

    if(x>0){

      // Create new actor
      val newSpawned = context.actorOf(Props[CountActor])

      // Send it a list of process
      newSpawned ! list(x-1)
      spawnActors(list, x-1)
    }
  }

  /**Combines a hash table to main hash table
   *
   *  @param newMap hash table sent by count actors
   */
  def combineCounts(newMap : Map[Char, Int]) : Unit = {

    // Keep track of how many more actors its waiting for
    awaitingCount -= 1

    // for each key value pair
    for((k,v) <- newMap) {
    {
      // if key exists
      if (charCount.contains(k)) {

        // combine key values
        val temp = charCount(k) + v
        charCount += (k -> temp)
      }
      else
      {
        // create new key
        charCount += (k -> v)
      }
    }

    // checks if all spawned actors finished processing
    }
    if (awaitingCount == 0) {
      printResult(charCount)
    }
  }

  /** Prints key and how many times the key occurred
   *
   *  @param result hash table to display
   */
  def printResult(result : Map[Char, Int]) : Unit ={

    // Display key and its value
    result.keys.foreach { i =>
      print("Key = " + i)
      println(" Value = " + result(i))
    }
  }
}

/** Count actors process a list and count how many times each character occurred
 *
 */
class CountActor extends Actor{

  /** Handles messages
   *
   */
  def receive: Receive = {
        // Sends processed list back to parent actor
    case s: List[String] => sender() ! processList(s)
  }

  /** Creates a hashmap to store processed list results
   *
   *  @param l list to process
   *  @return hashmap storing each character that occurred in list and how many times it occurred
   */
  def processList (l : List[String]): Map[Char, Int] = {

    // create new hashmap
    var charCount : Map[Char, Int] = Map()

    // process list
    charCount = iterateList(l, charCount)
    charCount

  }

  /** Iterates through list and counts occurrence of chars in each
   *
   *  @param l list to process
   *  @param map hashmap to store results
   *  @return hashmap storing each character that occurred in list and how many times it occurred
   */
  def iterateList (l : List[String], map : Map[Char, Int]): Map[Char, Int] = {
    l match {
      case Nil => map
      case x :: xs => iterateList(xs,countCharsInLine(x,map))
    }
  }

  /** Iterates through line and counts occurrence of chars in each
   *
   *  @param line string to process
   *  @param map hashmap to store results
   *  @return count of key and how many times it occurred
   */
  def countCharsInLine(line: String, map : Map[Char, Int]): Map[Char, Int] = {

    var charCount = map

    // for every char in string
    for (c <- line)
    {

      // if char as key exists
      if (charCount.contains(c))
      {
        // increment value
        val temp = charCount(c) + 1
        charCount += (c -> temp)
      }
      else
      {
        // create new key
        charCount += (c -> 1)
      }
    }
    charCount
  }
}


/** Object to read in a a file and then process using actors to
 * count how many times each character in file occurred
 *
 */
object CharacterCounter{
  def main(args: Array[String])
  {
    runCountActors()
  }

  /** Reads in file and stores it as a list
   *
   *  @param filename file name to read
   *  @return read in file as a list
   */
  def readFile(filename: String): List[String] = {

    // read file and turn into list
    val bufferedSource = io.Source.fromFile(filename)
    val lines = (for (line <- bufferedSource.getLines()) yield line).toList
    bufferedSource.close
    lines
  }


  /** Sends read in file to be processed by actors
   *
   */
  def runCountActors(): Any = {

    // read file
    val list = readFile("test.txt")

    // create actor system
    val system = ActorSystem("Counters")
    val a = system.actorOf(Props[SpawnActor], name="Spawn-Actor")

    // send list to actor
    a ! list
    Thread.sleep(1000)
    system.terminate()
  }
}






