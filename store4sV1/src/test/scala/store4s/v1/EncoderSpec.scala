package store4s.v1

import com.google.datastore.v1.ArrayValue
import com.google.datastore.v1.Entity
import com.google.datastore.v1.Key
import com.google.datastore.v1.PartitionId
import com.google.datastore.v1.Value
import com.google.protobuf.NullValue
import java.time.LocalDate
import org.scalatest.OneInstancePerTest
import org.scalatest.flatspec.AnyFlatSpec
import scala.jdk.CollectionConverters._

class EncoderSpec extends AnyFlatSpec with OneInstancePerTest {

  implicit val datastore = Datastore("store4s")
  def entityBuilder(kind: String) = Entity
    .newBuilder()
    .setKey(
      Key
        .newBuilder()
        .setPartitionId(
          PartitionId
            .newBuilder()
            .setProjectId("store4s")
            .build()
        )
        .addPath(
          Key.PathElement
            .newBuilder()
            .setKind(kind)
            .setName("entityName")
        )
    )

  "An v1.EntityEncoder" should "generate same output as Google Cloud Java" in {
    val userG = entityBuilder("User")
      .putProperties("id", Value.newBuilder().setIntegerValue(1).build())
      .putProperties(
        "name",
        Value.newBuilder().setStringValue("Sakura Minamoto").build()
      )
      .putProperties("admin", Value.newBuilder().setBooleanValue(true).build())
      .build()

    case class User(id: Int, name: String, admin: Boolean)
    val userS = User(1, "Sakura Minamoto", true).asEntity("entityName")

    assert(userG == userS)
  }

  it should "support nullable value" in {
    val userG = entityBuilder("User")
      .putProperties(
        "name",
        Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build()
      )
      .build()

    case class User(name: Option[String])
    val userS = User(None).asEntity("entityName")

    assert(userG == userS)
  }

  it should "support list value" in {
    val list = Seq("A", "B", "C")
    val arrayValue = ArrayValue
      .newBuilder()
      .addAllValues(
        list
          .map(e => Value.newBuilder().setStringValue(e).build())
          .asJava
      )
      .build()
    val groupG = entityBuilder("Group")
      .putProperties("id", Value.newBuilder().setIntegerValue(1).build())
      .putProperties(
        "members",
        Value.newBuilder().setArrayValue(arrayValue).build()
      )
      .build()

    case class Group(id: Int, members: Seq[String])
    val groupS = Group(1, list).asEntity("entityName")

    assert(groupG == groupS)
  }

  it should "support nested entity" in {
    val hometown = Entity
      .newBuilder()
      .putProperties(
        "country",
        Value.newBuilder().setStringValue("Japan").build()
      )
      .putProperties(
        "city",
        Value.newBuilder().setStringValue("Saga").build()
      )
      .build()
    val userG = entityBuilder("User")
      .putProperties(
        "name",
        Value.newBuilder().setStringValue("Sakura Minamoto").build()
      )
      .putProperties(
        "hometown",
        Value.newBuilder().setEntityValue(hometown).build()
      )
      .build()

    case class Hometown(country: String, city: String)
    case class User(name: String, hometown: Hometown)
    val userS =
      User("Sakura Minamoto", Hometown("Japan", "Saga")).asEntity("entityName")

    assert(userG == userS)
  }

  "A v1.ValueEncoder" should "support contramap" in {
    val userG = entityBuilder("User")
      .putProperties(
        "name",
        Value.newBuilder().setStringValue("Sakura Minamoto").build()
      )
      .putProperties(
        "birthday",
        Value.newBuilder().setStringValue("1991-04-02").build()
      )
      .build()

    implicit val enc =
      ValueEncoder.stringEncoder.contramap[LocalDate](_.toString())

    case class User(name: String, birthday: LocalDate)
    val userS =
      User("Sakura Minamoto", LocalDate.of(1991, 4, 2)).asEntity("entityName")

    assert(userG == userS)
  }
}
