package io.github.ppdzm.himma.scala

import io.github.ppdzm.himma.scala.component.Himma
import io.github.ppdzm.himma.scala.config.HimmaConfig
import io.github.ppdzm.utils.universal.config.{Config, FileConfig}

/**
 * Created by Stuart Alex on 2016/11/25.
 * MySQL——HBase、MySQL、Kafka同步Canal Client
 */
object HimmaLauncher extends App {
    private implicit val config: Config = new FileConfig(args)
    private implicit val himmaConfig: HimmaConfig = new HimmaConfig(config)
    new Himma().start()
}
