import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file._
import java.text.SimpleDateFormat
import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.pdfbox.multipdf.Splitter
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper

import scala.beans.BeanProperty
import scala.collection.JavaConversions._

class PDFMetaInfo(@BeanProperty var Author:String,
                   @BeanProperty var Title :String,
                   @BeanProperty var KeyWords:String,
                   @BeanProperty var Subject :String,
                   @BeanProperty var CreationDate:String)

object PDFtxtDaemon {

	System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")

	def main(args: Array[String]) {

		val start = System.currentTimeMillis()
		val argcount = args.length
		var counter = 0
		val log = "pdf.log.txt"

		if (args.length > 0) {

			args.foreach((filename) => {
				  val fname = new File(filename)

					if (fname.isDirectory) {
						val pagemaker = new ObjectMapper()

						val metafilename = Paths.get(filename,"pdfs.metadata.json")

						val metadata =
							   if(new File(metafilename.toString).exists()) {
									val metasfile = Files.readAllBytes(metafilename)

									pagemaker.readValue(new String(metasfile, "UTF-8"), classOf[util.HashMap[String, PDFMetaInfo]])
							   }
							   else {
								   new util.HashMap[String, PDFMetaInfo]()
							   }

						val files = fname.listFiles().filter(f => f.getName.toLowerCase().takeRight(4) == ".pdf")
						val metas =
								files.map(f => {
									counter+= 1
									processfile(start, counter, files.length, log, f, isdir = true)
									//processMetaInfo(counter, f)
								})

						metas.filter(_.isDefined).foreach({
									case Some((filepath,m)) =>
											  try {
												  metadata.put(filepath,m)
											  } catch {
												  case e:Throwable => println("Error on " + filepath)
											  }
						})

						Files.write(metafilename, pagemaker.writeValueAsString(metadata).getBytes(StandardCharsets.UTF_8))
					}
					else {
						counter += 1
						processfile(start, counter, argcount, log, fname, isdir = false)
					}
			})
		}
		else {
			openCommunicationChannel()
		}
	}

	def nullCheck (str : String): String = {
		if (str != null) str else ""
	}


	def processMetaInfo (count : Int, filepath: File): Option[(String,PDFMetaInfo)] = {
		var doc = new PDDocument()
		var metainf : Option[(String,PDFMetaInfo)] = None
		try {
			doc = PDDocument.load(filepath)
			val docinfo = doc.getDocumentInformation

			val df = new SimpleDateFormat("yyyy/MM/dd")
			val date = df.format(docinfo.getCreationDate.getTime)
			val title = if (docinfo.getTitle != null) docinfo.getTitle else ""
			val metainfo = new PDFMetaInfo(docinfo.getAuthor, title,nullCheck(docinfo.getKeywords), nullCheck(docinfo.getSubject), date)

			println(count + ": " + filepath.getName)
			println(metainfo.Title)
			println(metainfo.KeyWords)
			println(metainfo.Subject)
			println("")

			metainf = Some((filepath.getName, metainfo))
		}
		catch {
			case e: Throwable =>
				println("ERROR! " + e.getMessage)
		} finally {
			if (doc != null) doc.close()
		}
		metainf
	}

	def processfile(start: Long, c: Int, count: Int, log: String, filepath: File, isdir : Boolean): Option[(String,PDFMetaInfo)] = {
		val dir = filepath.getParent
		println("Starting on: " + filepath)

		val newfileName = filepath.getName.replace(".pdf", ".txt")
		val fname = if(isdir) Paths.get(dir,"PDFs-Text",newfileName) else Paths.get(filepath.toString.replace(".pdf", ".txt"))
	  var metainf : Option[(String,PDFMetaInfo)] = None

		if (!new File(fname.toString).exists()) {
			val pagemaker = new ObjectMapper()
			val stripper = new PDFTextStripper()
			val splitter = new Splitter()

			var doc = new PDDocument()

			println(c + "/" + count + " " + filepath)

			val time0 = System.currentTimeMillis()

			try {
				doc = PDDocument.load(filepath)
				splitter.setSplitAtPage(1)

				val docinfo = doc.getDocumentInformation

				val df = new SimpleDateFormat("yyyy/MM/dd")
				val date = df.format(docinfo.getCreationDate.getTime)

				val metainfo = new PDFMetaInfo(nullCheck(docinfo.getAuthor)  , nullCheck(docinfo.getTitle)  ,
					                              nullCheck(docinfo.getKeywords), nullCheck(docinfo.getSubject), date)

				metainf = Some((filepath.getName, metainfo))

				val splitdoc = splitter.split(doc)
				val pages = new util.ArrayList(splitdoc.map(p => stripper.getText(p)))

				println("  => " + newfileName)

				Files.write(fname, pagemaker.writeValueAsString(pages).getBytes(StandardCharsets.UTF_8))

				Files.write(Paths.get(log), (filepath + ", " + ((System.currentTimeMillis() - start) / (1000.0 * 60.0))).getBytes)

				val now = System.currentTimeMillis()
				val tot = (now - start) / (1000.0 * 60.0)
				val pr = (now - time0) / 1000.0

				println("Took " + pr + " seconds")
				println("Total mins: " + tot)
				println()

				doc.close()
			}
			catch {
				case error: Throwable => err(error,filepath)

			} finally {
				if (doc != null) doc.close()
			}
		}
		metainf
	}

	def err(error:Throwable, filepath:File):Unit= {
		val msg = error.getMessage
		val m = if(msg==null) "unknown error" else msg

		println(error)

		val b = m.getBytes(StandardCharsets.UTF_8)
		Files.write(Paths.get(filepath + ".error"), b)
	}

	def openCommunicationChannel(): Unit ={
		var pipebox: Option[RandomAccessFile] = None

		val stripperg = new PDFTextStripper()

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
										val errmsg = "[ERROR]: " + e.getMessage
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
