package com.lambdalab.jgit.jdbc.test

import java.io.Closeable
import java.util.Collections
import java.util.concurrent.TimeUnit

import com.github.dockerjava.api.async.ResultCallback
import com.github.dockerjava.api.command.CreateContainerResponse
import com.github.dockerjava.api.model.{ExposedPort, Frame, Ports}
import com.github.dockerjava.core.command.{LogContainerResultCallback, PullImageResultCallback}
import com.github.dockerjava.core.{DefaultDockerClientConfig, DockerClientBuilder}
import com.github.dockerjava.core.async.ResultCallbackTemplate

import scala.collection.JavaConverters._

object DockerTool {

  lazy val config = DefaultDockerClientConfig.createDefaultConfigBuilder()
  lazy val docker = DockerClientBuilder.getInstance(config).build()

  def startContainer(name: String, image: String, ports: Map[Int, Int], envs: Map[String, String], cmds: String*): String = {
//    docker.pullImageCmd(image).exec(new PullImageResultCallback).awaitSuccess()
    val exists = docker.listContainersCmd().withShowAll(true).exec().asScala.find{
      container =>
      container.getNames.contains(s"/$name")
    }
    if(exists.isDefined) {
      val id = exists.get.getId
      if (!exists.get.getStatus.startsWith("Up")){
        docker.removeContainerCmd(id).exec()
      } else {
        return id
      }
    }
    val c = docker.createContainerCmd(image).withName(name)
    val bindings = new Ports()
    val exposes = new java.util.ArrayList[ExposedPort]()
    ports.foreach {
      case (exposed, bindTo) =>
        val e = ExposedPort.tcp(exposed)
        exposes.add(e)
        bindings.bind(e, Ports.Binding.bindPort(bindTo))
    }
    c.withExposedPorts(exposes)
    c.withPortBindings(bindings)
    c.withEnv(envs.map(e => s"${e._1}=${e._2}").toSeq: _*)
    if (cmds.nonEmpty) {
      c.withCmd(cmds:_*)
    }

    val id = c.exec().getId
    docker.startContainerCmd(id).exec()
    id
  }

  def tailContainer(id: String, until: String): Unit = {
    val lock = new Object()
    docker.logContainerCmd(id).withTimestamps(true)
            .withStdOut(true)
            .withStdErr(true)
            .withFollowStream(true)
            .withTailAll()
        .exec(new ResultCallbackTemplate[LogContainerResultCallback, Frame]{
          override def onNext(item: Frame): Unit = {
            val line = item.toString
            println(line)
            if(line.contains(until)) lock.synchronized({
              lock.synchronized({lock.notifyAll()})
              println("found ready str! start running tests.")
            })
          }
        })
    lock.synchronized({lock.wait(60*1000)})
  }

  def stopContainer(id: String): Unit = {
    docker.stopContainerCmd(id).exec()
    docker.removeContainerCmd(id).withForce(true).exec()
  }
}
