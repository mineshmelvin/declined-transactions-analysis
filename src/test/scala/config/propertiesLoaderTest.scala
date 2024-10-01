package config

import org.junit.Assert._
import org.junit.Test
import org.mockito.Mockito._

import java.io.{FileInputStream, IOException}
import self.training.config.propertiesLoader.loadProperties

class propertiesLoaderTest {

  @Test
  def testLoadPropertiesSuccess(): Unit = {

    val loadedProperties = loadProperties("C:\\Users\\mines\\workspace\\projects\\training\\radar_replica\\src\\main\\resources\\application.conf")

    // Verify the contents of the properties object
    assertNotNull(loadedProperties)
    assertEquals("transactions", loadedProperties.getProperty("kafka.input.topic.transaction"))
    assertEquals("CreditCardTransactionProcessing", loadedProperties.getProperty("spark.app.name"))
  }

  @Test(expected = classOf[IOException])
  def testLoadPropertiesFailure(): Unit = {
    // Simulate an exception being thrown when reading the file
    val mockFileInputStream = mock(classOf[FileInputStream])
    doThrow(new IOException("File not found")).when(mockFileInputStream).read()

    // Call the method and expect an exception
    loadProperties("nonExistentFilePath")
  }
}

