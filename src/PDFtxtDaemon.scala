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

	System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");

	def main(args: Array[String]) {

		val start = System.currentTimeMillis()
		val count = args.length
		var c = 0
		val log = "pdf.log.txt"

		val stripperg = new PDFTextStripper()

		if (args.length > 0) {
			args.foreach((filename) => {
				val fname = new File(filename)

				if (fname.isDirectory()) {
					val files = fname.listFiles().filter(f => f.getName().toLowerCase().contains(".pdf"))
					files.par.foreach(f => {
						c+= 1
						processfile(start, c, files.length, log, f, true)
					})
				}
				else {
					c += 1
					processfile(start, c, count, log, fname, false)
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
						catch {
							case _ => ()
						}
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
									val errmsg = "[ERROR]: " + e.getMessage()
									val txtbytes = errmsg.getBytes(StandardCharsets.UTF_8)
									val plen = ByteBuffer.allocate(4).putInt(txtbytes.length).array()
									pipe.write(plen)
									pipe.write(txtbytes)
									println(errmsg)
							}
						}
				}
			}
		}
	}

	def processfile(start: Long, c: Int, count: Int, log: String, filepath: File, isdir : Boolean): Unit = {

		val newfileName = filepath.getName().replace(".pdf", ".txt")
		if (!(new File(newfileName).exists())) {
			val pagemaker = new ObjectMapper()
			val stripper = new PDFTextStripper()
			val splitter = new Splitter()

			var doc = new PDDocument()

			println(c + "/" + count + " " + filepath)

			val x0 = System.currentTimeMillis()
			try {

				doc = PDDocument.load(filepath)
				splitter.setSplitAtPage(1)

				val pages = new util.ArrayList(splitter.split(doc).map(p => stripper.getText(p)))

				val dir = filepath.getParent()

				val fname = if(isdir) Paths.get(dir,"PDFs-Text",newfileName) else Paths.get(filepath.toString().replace(".pdf", ".txt"))

				println("  => " + newfileName)

				Files.write(fname, pagemaker.writeValueAsString(pages).getBytes(StandardCharsets.UTF_8))

				Files.write(Paths.get(log), (filepath + ", " + ((System.currentTimeMillis() - start) / (1000.0 * 60.0))).getBytes)

				val now = System.currentTimeMillis()
				val tot = (now - start) / (1000.0 * 60.0)
				val pr = (now - x0) / 1000.0

				println("Took " + pr + " seconds")
				println("Total mins: " + tot)
				println()

				doc.close()
			}
			catch {
				case e: Throwable =>
					Files.write(Paths.get(filepath + ".error"), e.getMessage.getBytes(StandardCharsets.UTF_8))
			} finally {
				if (doc != null) doc.close()
			}
		}
	}
}
