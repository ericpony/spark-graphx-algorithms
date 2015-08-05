package graphx

import org.apache.log4j.Logger;
import org.apache.log4j.Level

trait Logger {
  private val loggerName = this.getClass.getName
  private lazy val logger = Logger.getLogger(loggerName)
  def info(msg: => String) = logger.info(msg)
  def warn(msg: => String) = logger.warn(msg)
  def debug(msg: => String) = logger.debug(msg)
  def setLevel(level: Level) = logger.setLevel(level)
  setLevel(Level.INFO)
}
