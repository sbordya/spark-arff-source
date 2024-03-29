package handson.conf

import pureconfig.ConfigSource
import pureconfig.generic.auto._

object Configuration {

  val config: ServiceConf = ConfigSource.default.load[ServiceConf] match {
    case Right(conf) => conf
    case Left(error) => throw new Exception(error.toString())
  }
}
