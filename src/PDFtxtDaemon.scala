import java.io.{File, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file._
import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.util.{Splitter, PDFTextStripper}
import scala.collection.JavaConversions._

object PDFtxtDaemon {
		def main(args: Array[String]) {

			val start = System.currentTimeMillis()
			val count = args.length
			var c = 1
			val log = "pdf.log.txt"
			val stripperg = new PDFTextStripper()

			if (args.length > 0) {
				args.par.foreach((a) => {

					val pagemaker = new ObjectMapper();
					val stripper = new PDFTextStripper()
					val splitter = new Splitter()

					val newpath = a.replace(".pdf", ".txt")
					println(c + "/" + count + " " + a)
					c += 1
					println("  => " + newpath)
					Files.write(Paths.get(log), (a + ", " + ((System.currentTimeMillis() - start) / (1000.0 * 60.0))).getBytes)

					val x0 = System.currentTimeMillis()
					var doc = new PDDocument()

					try {
						doc = PDDocument.load(a)
						splitter.setSplitAtPage(1)

						val pages = new util.ArrayList(splitter.split(doc).map(p => stripper.getText(p)))

						Files.write(Paths.get(newpath), pagemaker.writeValueAsString(pages).getBytes(StandardCharsets.UTF_8))

						doc.close()

						val now = System.currentTimeMillis()
						val tot = (now - start) / (1000.0 * 60.0)
						val pr = (now - x0) / 1000.0

						println("Took " + pr + " seconds")
						println("Total mins: " + tot)
						println()
					}
					catch {
						case e: Throwable =>
							Files.write(Paths.get(a + ".error"), e.getMessage.getBytes(StandardCharsets.UTF_8))
					} finally {
						if (doc != null) doc.close()
					}
				})
			}
			else {
				var die = false
				while (!die) {
					val inp = scala.io.StdIn.readLine()
					if (inp == "DIE") die = true
					else {
						try {
							val doc = PDDocument.load(inp)
							val txt = stripperg.getText(doc)
							doc.close()

							System.setOut(new PrintStream(System.out, true, "UTF-8"))
							println(txt)
						}
						catch {
							case e: Throwable =>
								println("ERROR LOADING PDF")
						}
					}
				}
			}
		}
	}

