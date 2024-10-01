package self.training.config

import java.io.FileInputStream
import java.util.Properties

object propertiesLoader {

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
