package self.training.config

import java.io.FileInputStream
import java.util.Properties

object propertiesLoader {
  /**
   * Used to read a configuration and return a Properties object of those configurations to be used in the application
   * @param filePath Path to the configuration file contains required configurable  values used in the application
   * @return java.util.Properties
   */

  def loadProperties(filePath: String): Properties = {
    val properties = new Properties()
    val inputStream = new FileInputStream(filePath)
    try{
      properties.load(inputStream)
    } finally {
      inputStream.close()
    }
    properties
  }
}
