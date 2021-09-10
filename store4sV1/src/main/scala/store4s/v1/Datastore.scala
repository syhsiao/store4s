package store4s.v1

import com.google.datastore.v1.client.DatastoreFactory
import com.google.datastore.v1.client.DatastoreHelper
import com.google.datastore.v1.client.DatastoreOptions
import com.google.datastore.v1.client.{Datastore => GDatastore}
import com.google.datastore.v1.CommitRequest
import com.google.datastore.v1.Entity
import com.google.datastore.v1.Key
import com.google.datastore.v1.Mutation

case class Datastore(
    options: DatastoreOptions,
    namespace: Option[String] = None
) {
  val projectId: String = options.getProjectId()
  val underlying: GDatastore = DatastoreFactory.get().create(options)

  def add(entity: Entity) = {
    val request = CommitRequest.newBuilder()
      .addMutations(Mutation.newBuilder().setInsert(entity))
      .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
      .build()
    underlying.commit(request).getMutationResults(0).getKey()
  }

  def update(entity: Entity) = {
    val request = CommitRequest.newBuilder()
      .addMutations(Mutation.newBuilder().setUpdate(entity))
      .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
      .build()
    underlying.commit(request).getMutationResults(0).getKey()
  }

  def put(entity: Entity) = {
    val request = CommitRequest.newBuilder()
      .addMutations(Mutation.newBuilder().setUpsert(entity))
      .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
      .build()
    underlying.commit(request).getMutationResults(0).getKey()
  }

  def delete(key: Key) = {
    val request = CommitRequest.newBuilder()
      .addMutations(Mutation.newBuilder().setDelete(key))
      .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
      .build()
    underlying.commit(request).getMutationResults(0).getKey()
  }

  def delete(kind: String, name: String) = {
    val key = DatastoreHelper.makeKey(kind, name)
    val request = CommitRequest.newBuilder()
      .addMutations(Mutation.newBuilder().setDelete(key))
      .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
      .build()
    underlying.commit(request).getMutationResults(0).getKey()
  }
}

object Datastore {
  def defaultInstance = Datastore(DatastoreHelper.getOptionsFromEnv().build())

  def apply(projectId: String): Datastore = Datastore(
    DatastoreHelper.getOptionsFromEnv().projectId(projectId).build()
  )
}
