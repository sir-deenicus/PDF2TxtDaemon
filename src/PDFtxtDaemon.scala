import java.io.{RandomAccessFile, File, PrintStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file._
import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.pdfbox.multipdf.Splitter
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper

import scala.collection.JavaConversions._

object PDFtxtDaemon {
	def main(args: Array[String]) {

		val start = System.currentTimeMillis()
		val count = args.length
		var c = 1
		val log = "pdf.log.txt"
		val stripperg = new PDFTextStripper()

		if (args.length > 0) {
			args.par.foreach((filename) => {

				val pagemaker = new ObjectMapper()
				val stripper = new PDFTextStripper()
				val splitter = new Splitter()

				val newpath = filename.replace(".pdf", ".txt")
				println(c + "/" + count + " " + filename)
				c += 1
				println("  => " + newpath)
				Files.write(Paths.get(log), (filename + ", " + ((System.currentTimeMillis() - start) / (1000.0 * 60.0))).getBytes)

				val x0 = System.currentTimeMillis()
				var doc = new PDDocument()

				try {

					doc = PDDocument.load(new File(filename))
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
						Files.write(Paths.get(filename + ".error"), e.getMessage.getBytes(StandardCharsets.UTF_8))
				} finally {
					if (doc != null) doc.close()
				}
			})
		}
		else {
			var pipebox: Option[RandomAccessFile] = None

			var die = false
			while (!die) {
				pipebox match {
					case None =>
						try {
							val pipe = new RandomAccessFile("\\\\.\\pipe\\pdfDaemon-commpipe", "rw")
							pipebox = Some(pipe)
						}
						catch {case _ => ()}
					case Some(pipe) =>
						val inp = pipe.readInt()

						if (inp == 12) die = true
						else {
							try {
								val fsize = pipe.readInt()
								val fileBytes = new Array[Byte](fsize)
								val len = pipe.read(fileBytes)

								val doc = PDDocument.load(fileBytes)
								val txt = stripperg.getText(doc)
								doc.close()

								val txtbytes = txt.getBytes(StandardCharsets.UTF_8)
								val plen = ByteBuffer.allocate(4).putInt(txtbytes.length).array()
								pipe.write(plen)
								pipe.write(txtbytes)
							}
							catch {
								case e: Throwable =>
									println(e.getMessage())
							}
						}
				}
			}
		}
	}
}
