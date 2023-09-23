import scala.collection.immutable.{List, Map}
import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorSystem


/*
  Goal Spawn Actor Class

  Receives list of strings and based the size of the list,
  it determines how many Goal Count Actors it needs to spawn and allocates a
  part of the list to the each of them

  Calculates number of goals each time scored

 */
class GoalSpawnActor extends Actor{

  var goalCount : Map[String, Int] = Map()
  var awaitingCount : Int = 0

  // Handles messages
  def receive: Receive = {
    case s: List[String] => determineHowManyActors(s)
    case s: Map[String, Int] => combineCounts(s)
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

  /** Spawns Goal Count Actors and sends a list for them to process
   *
   *  @param list list which the actor will process
   *  @param x how many more actors to spawn
   */
  def spawnActors(list : List[List[String]], x: Int): Unit = {

    if(x>0){

      // Create new actor
      val newSpawned = context.actorOf(Props[GoalCountActor])

      // Send it a list of process
      newSpawned ! list(x-1)
      spawnActors(list, x-1)
    }
  }

  /**Combines a hash table to main hash table
   *
   *  @param newMap hash table sent by count actors
   */
  def combineCounts(newMap : Map[String, Int]) : Unit = {

    // Keep track of how many more actors its waiting for
    awaitingCount -= 1

    // for each key value pair
    for((k,v) <- newMap) {
      {
        // if key exists
        if (goalCount.contains(k)) {

          // combine key values
          val temp = goalCount(k) + v
          goalCount += (k -> temp)
        }
        else
        {
          // create new key
          goalCount += (k -> v)
        }
      }

      // checks if all spawned actors finished processing
    }
    if (awaitingCount == 0) {
      printResult(goalCount)
    }
  }

  /** Prints key and how many times the key occurred
   *
   *  @param result hash table to display
   */
  def printResult(result : Map[String, Int]) : Unit ={

    // Display key and its value
    result.keys.foreach { i =>
      print("Team [" + i)
      println("\t\t:" + result(i) + "]")
    }
  }
}

/** Goal Count actors process a list and count how many times each team scored
 *
 */
class GoalCountActor extends Actor{

  /** Handles messages
   *
   */
  def receive: Receive = {
    // Sends processed list back to parent actor
    case s: List[String] => sender() ! processList(s)
  }

  /** Creates a hashmap to store processed list results
   *
   *  @param list list to process
   *  @return hashmap storing each team that occurred in list and how many times it scored
   */
  def processList (list : List[String]): Map[String, Int] = {

    // create new hashmap
    var charCount : Map[String, Int] = Map()

    // process list
    charCount = iterateList(list, charCount)
    charCount

  }

  /** Iterates through line, check if teams exist,
   * if it does, add it goals to the key value
   * if not create new key value pair with key as team name and value as its goals
   *
   *  @param line string to process
   *  @param goalsCount hashmap to store results
   *  @return count of key and how many times it occurred
   */
  def processLine(line: String, goalsCount : Map[String, Int]): Map[String, Int] = {
    var temp = goalsCount
    val s = line.split(",")

    // For both home and away team
    for( a <- 2 to 3){

      // if team as key exists
      if (temp.contains(s(a)))
      {
        // increment value with teams new goals
        val tempVal =  temp(s(a)) + s(a + 2).toInt
        temp += (s(a) -> tempVal)
      }
      else
      {
        // create new key
        temp += (s(a) -> s(a + 2).toInt)
      }
    }
    temp
  }

  /** Iterates through list and process each line
   *
   *  @param list list to iterate through to process
   *  @param map hashmap to store results
   *  @return count team and how many times they scored
   */
  def iterateList (list : List[String], map : Map[String, Int]): Map[String, Int] = {
    list match {
      case Nil => map
      case x :: xs => iterateList(xs,processLine(x,map))
    }
  }
}


/** Reads in multiple files, sends the files to SpawnActor to found how
 * many goals each time scored
 *
 */
object FootballGoals {
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
    val file1 = readFile("season-1819_csv.csv")
    val file2 = readFile("season-1718_csv.csv")
    val file3 = readFile("season-1617_csv.csv")
    val file4 = readFile("season-1516_csv.csv")
    val file5 = readFile("season-1415_csv.csv")

    // Combine files
    val list = file1.tail:::file2.tail:::file3.tail:::file4.tail:::file5.tail

    // create actor system
    val system = ActorSystem("Goal_Counters")
    val a = system.actorOf(Props[GoalSpawnActor], name="Spawn-Actor")

    // send list to actor
    a ! list
    Thread.sleep(1000)
    system.terminate()
  }

}
