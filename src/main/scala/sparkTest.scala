import com.mongodb.client.{FindIterable, MongoCollection, MongoDatabase}
import com.mongodb.{BasicDBObject, DBCollection, MongoClient}
import com.mongodb.hadoop.io.MongoUpdateWritable
import org.apache.hadoop.conf.Configuration
import org.bson.{BSONObject, BasicBSONObject, Document}
import com.mongodb.hadoop.{MongoInputFormat, MongoOutputFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

object sparkTest extends App {

  //setting up mongo
  val mongo: MongoDatabase = new MongoClient("localhost",27017).getDatabase("test")
  var source: MongoCollection[Document] = mongo.getCollection("source")
  val target: MongoCollection[Document] = mongo.getCollection("target")
  source.drop()
  target.drop()
  //inserting document
  val sourceDoc = new Document()
  sourceDoc.put("unchanged","this field should not be changed")
  sourceDoc.put("_id","1")
  source.insertOne(sourceDoc)

  //setting up spark
  val conf = new SparkConf().setAppName("test mongo with spark").setMaster("local")
  val mongoConfig = new Configuration()
  val sc = new SparkContext(conf)
  mongoConfig.set("mongo.input.uri",
    "mongodb://localhost:27017/test.source")
  mongoConfig.set("mongo.output.uri",
    "mongodb://localhost:27017/test.target")

  //setting up read
  val documents = sc.newAPIHadoopRDD(
    mongoConfig,                // Configuration
    classOf[MongoInputFormat],  // InputFormat
    classOf[Object],            // Key type
    classOf[BSONObject])        // Value type

  //building updates with no document matching the query in the target collection
  val upsert_insert_rdd: RDD[(Object, MongoUpdateWritable)] = documents.mapValues(
    (value: BSONObject) => {

      val query = new BasicBSONObject
      query.append("_id", value.get("_id").toString)

//      val update = new BasicBSONObject()//value.asInstanceOf[BasicBSONObject])
//      val changes = new BasicBSONObject()
//      val keys = value.asInstanceOf[BasicBSONObject].keySet()
//      keys.foreach(key => {
//        changes.append(key,value.get(key))
//      })
//      changes.append("added","this data will be added")
//
//      update.append("$set",changes)


      val update = new BasicBSONObject()//value.asInstanceOf[BasicBSONObject])
      val changes = new BasicBSONObject()
      val keys = value.asInstanceOf[BasicBSONObject].keySet()
      keys.foreach(key => {
        //TODO The ID must be included for the first write, but cannot for the second.
        //if (key != "_id")
          changes.append(key,value.get(key))
      })
      changes.append("overwritten","this data will be removed")

      update.append("$set",changes)

      println("val:"+value.toString)
      println("query:"+query.toString)
      println("update:"+update.toString)

      new MongoUpdateWritable(
        query,  // Query
        update,  // Update
        true,  // Upsert flag
        false,   // Update multiple documents flag
        //TODO This is false using $set, when set to true and direct inserts changes no insert happens
        false  // Replace flag
      )}
  )
  //saving updates
  upsert_insert_rdd.saveAsNewAPIHadoopFile(
    "",
    classOf[Object],
    classOf[MongoUpdateWritable],
    classOf[MongoOutputFormat[Object, MongoUpdateWritable]],
    mongoConfig)

  // At this point, there should be a new document in the target database, but there is not.
  val count = target.count()
  println("count after insert: "+count+", expected: 1")

  //adding doc to display working update. This code will throw an exception if there is a
  //document with a matching _id field in the collection, so if this breaks that means the upsert worked!
  //val targetDoc = new Document()
  //targetDoc.put("overwritten","this field should not be changed")
  //targetDoc.put("_id","1")
  //target.insertOne(targetDoc)

  //building updates when a document matching the query exists in the target collection
  val upsert_update_rdd: RDD[(Object, MongoUpdateWritable)] = documents.mapValues(
    (value: BSONObject) => {

      val query = new BasicBSONObject
      query.append("_id", value.get("_id").toString)

      val update = new BasicBSONObject()//value.asInstanceOf[BasicBSONObject])
      val changes = new BasicBSONObject()
      val keys = value.asInstanceOf[BasicBSONObject].keySet()
      keys.foreach(key => {
        //TODO: The ID cannot be included here, since it cannot be set, but must be included for the first insert. 
        if (key != "_id")
          changes.append(key,value.get(key))
      })
      changes.append("added","this data was added")

      update.append("$set",changes)
      //update.append("_id",value.get("_id"))
      println("val:"+value.toString)
      println("query:"+query.toString)
      println("update:"+update.toString)

      new MongoUpdateWritable(
        query,  // Query
        update,  // Update
        true,  // Upsert flag
        false,   // Update multiple documents flag
        //TODO this will replace successfully. When set replace to false and
        false  // Replace flag
      )}
  )
  //saving updates
  upsert_update_rdd.saveAsNewAPIHadoopFile(
    "",
    classOf[Object],
    classOf[Object],
    classOf[MongoOutputFormat[Object, Object]],
    mongoConfig)

  //checking that the update succeeded. should print:
  //contains new field:true, contains overwritten field:false
  val ret = target.find().first
  if (ret != null)
    println("contains new field:"+ret.containsKey("added")+", contains overwritten field:"+ret.containsKey("overwritten"))
  else
    println("no documents found in target")


}
