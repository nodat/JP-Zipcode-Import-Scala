import org.scalaquery._
import org.scalaquery.session.{Database,Session}
import org.scalaquery.ql._
//import org.scalaquery.ql.basic.{BasicTable => Table}
//import org.scalaquery.ql.basic.BasicDriver.Implicit._
import org.scalaquery.ql.extended.{ExtendedTable => Table}
import org.scalaquery.ql.extended.MySQLDriver.Implicit._ 

import org.scalaquery.simple.StaticQuery

import scala.io.Source
import scala.util.control.Exception._	

import java.io._
import java.net.URL
import java.util.HashSet
import java.util.ArrayList
import java.util.Properties

import org.apache.commons.io._
import jp.gr.java_conf.turner.util.lha.LhaInputStream
import jp.sourceforge.csvparser._

/**
 * 郵便番号インポートアプリケーション
 * 1. 日本郵便サイトからファイルをダウンロード
 * 2. ファイルを解凍する
 * 3. CSVファイルを読み込みDBにインポート
 */

/**
 * zipcode case class
 * @see http://www.post.japanpost.jp/zipcode/dl/readme.html
 * 
 * この郵便番号データファイルでは、以下の順に配列しています。
 *  0: 全国地方公共団体コード(JIS X0401、X0402)………　半角数字
 *  1: (旧)郵便番号(5桁)………………………………………　半角数字
 *  2: 郵便番号(7桁)………………………………………　半角数字
 *  3: 都道府県名　…………　半角カタカナ(コード順に掲載)　(注1)
 *  4: 市区町村名　…………　半角カタカナ(コード順に掲載)　(注1)
 *  5: 町域名　………………　半角カタカナ(五十音順に掲載)　(注1)
 *  6: 都道府県名　…………　漢字(コード順に掲載)　(注1,2)
 *  7: 市区町村名　…………　漢字(コード順に掲載)　(注1,2)
 *  8: 町域名　………………　漢字(五十音順に掲載)　(注1,2)
 *
 * // 以下はデータ更新に必要な情報のため、テーブル情報には含めない。
 *  9: 一町域が二以上の郵便番号で表される場合の表示　(注3)　(「1」は該当、「0」は該当せず)
 * 10: 小字毎に番地が起番されている町域の表示　(注4)　(「1」は該当、「0」は該当せず)
 * 11: 丁目を有する町域の場合の表示　(「1」は該当、「0」は該当せず)
 * 12: 一つの郵便番号で二以上の町域を表す場合の表示　(注5)　(「1」は該当、「0」は該当せず)
 * 13: 更新の表示（注6）（「0」は変更なし、「1」は変更あり、「2」廃止（廃止データのみ使用））
 * 14: 変更理由　(「0」は変更なし、「1」市政・区政・町政・分区・政令指定都市施行、「2」住居表示の実施、「3」区画整理、「4」郵便区調整等、「5」訂正、「6」廃止(廃止データのみ使用))
 */
case class Zipcode(val areaCode:String,
	val oldZipcode:String,
	val zipcode:String,
	val prefKana:String,
	val cityKana:String,
	val townKana:String,
	val pref:String,
	val city:String,
	val town:String)
/**
 * zipcode data model  
 */
object ZipcodeDao extends Table[Zipcode]("jp_zipcode") {
	def id 			= column[Int]("id", O.PrimaryKey, O.AutoInc)
	def area_code 	= column[String]("area_code", O.NotNull, O DBType "varchar(5)")
	def old_zipcode = column[String]("old_zipcode", O.NotNull, O DBType "varchar(5)")
	def zipcode 	= column[String]("zipcode", O.NotNull, O DBType "varchar(7)")
	def pref_kana 	= column[String]("pref_kana", O DBType "varchar(128)")
	def city_kana 	= column[String]("city_kana", O DBType "varchar(128)")
	def town_kana 	= column[String]("town_kana", O DBType "varchar(128)")
	def pref	 	= column[String]("pref", O DBType "varchar(128)")
	def city 		= column[String]("city", O DBType "varchar(128)")
	def town 		= column[String]("town", O DBType "varchar(128)")
	def * = area_code ~ old_zipcode ~ zipcode ~ pref_kana ~ city_kana ~ town_kana ~ pref ~ city ~ town <> (Zipcode, Zipcode.unapply _)
		
	def findAll = for (z <- ZipcodeDao) yield z.zipcode
}

object ZipcodeImport {
  
	/**	
	 * main process
	 * @param args from command line
	 */
	def main(args:Array[String]):Unit = {
		// do process
		doProcess(args)
	}

	/**
	 * processing service
	 * @param args arguments args(0): application property filename
	 */
	def doProcess(args:Array[String]):Unit = {
		// 設定ファイルを取得
		val properties = args(0)
		val p = loadProperties(properties)
		// URLからファイルをダウンロード
		val filename = downloadFile(p.getProperty("url"), p.getProperty("workDir"))
		// ダウンロードしたファイルを解凍
		val extractFileList = extractFile(filename)
		// テーブルを作成
		val dburl 	 = p.getProperty("dburl")
		val driver 	 = p.getProperty("driver")
		val user 	 = p.getProperty("user")
		val password = p.getProperty("password")
		val db = Database.forURL(dburl, driver = driver, user = user, password = password)
		
		// ファイルを読み込み、レコード作成
		extractFileList.fold(
			e => { 
				printf("Error: %s", e)
			}, 
			fileList => {
				// DB 更新
				updateZipcodeTable(db, fileList)
			})
		// 終了
	}
	
	/**
	 * download file
	 * @param url 
	 * @param path
	 * @return filename
	 */
	def downloadFile(url:String, path:String):String = {
		val urlObject:URL = new URL(url)
		val filename:String = path + File.separator + new File(urlObject.getFile()).getName()
		// URL を開いて指定されたパスにファイルに書き出す
		using(urlObject.openStream()) { stream =>
			IOUtils.copy(stream, new FileOutputStream(filename)) 
		}
		printf("downloaded: %s\n", filename)
		return filename
	}

	/**
	 * extract lha file
	 * @param filename
	 * @return extracted filename
	 */	
	def extractFile(target:String):Throwable Either ArrayList[String] = {
		// 解凍ファイルのディレクトリ情報を整理
		val targetPath = new File(target).getParentFile()
		val baseDirName = new File(target).getName()
		val baseDirPath = targetPath
		
		// 作業ディレクトリの作成
		if (!baseDirPath.isDirectory()) {
			baseDirPath.mkdir()
		}
	
		// 解凍処理
		var extractFileList:ArrayList[String] = new ArrayList[String]()
		allCatch either {
			val lis = new LhaInputStream(new BufferedInputStream(new FileInputStream(new File(target))))
			using(lis) { ls =>
			  	// 候補となるファイル・ディレクトリを分別
				Iterator.continually(ls.getNextEntry).takeWhile(_ != null).filterNot(_.isDirectory).foreach(e => {
					// ファイルについて出力
					val file = new File(baseDirPath, e.getName)
					file.getParentFile.mkdir()
					// 書き出し
					val fos =  new FileOutputStream(file)
					using(fos) { fs =>
						Iterator.continually(ls.read()).takeWhile(_ != -1).foreach(fs write _)
						ls.closeEntry()
					}
					// 解凍したファイルリストを作成
					extractFileList.add(file.getAbsolutePath())
				})
			}
			extractFileList
		}
	}
	
	/**
	 * import file
	 * @param filelist
	 */	
	def updateZipcodeTable(db:Database, filelist:ArrayList[String], encoding:String = "SJIS"):Unit = {
		// 既存レコードのzipcodeキーを取得
		var currentTable = new HashSet[String]()
		db withSession { implicit s:Session =>
			for (zipcode <- ZipcodeDao.findAll) {
		  		currentTable.add(zipcode)
			}
		}
	  
		// 対象ファイルを１つずつ処理
		val listIterator = filelist.iterator();
		while (listIterator.hasNext()) {
			val filename = listIterator.next()
			printf("loading: %s\n", filename)
			
			// CSV ファイルを読み出し
			val utility = new BasicCSVUtility()	
			val reader = utility.createCSVReader(new BufferedReader(new InputStreamReader(new FileInputStream(new File(filename)), encoding)))
			var skip = new SkipState()

			// データを整理＆絞り込み処理
			val lines = Iterator.continually(reader.readRecord()).takeWhile(_ != null)
			lines.filter(line => {
				_normalize(line, skip)
			}).foreach(r => {
				// レコード処理
				db.withTransaction {
					implicit s:Session => {
						// 既存レコードの廃止の場合、削除
						if ("2".equals(r(13)) && currentTable.contains(r(2))) {
							ZipcodeDao.where(_.zipcode === r(2)).delete
						} else {
							// 更新レコードの場合、削除して追加
							if (currentTable.contains(r(2))) {
								ZipcodeDao.where(_.zipcode === r(2)).delete
							}
							// 新規の場合インサート
							ZipcodeDao.insert(new Zipcode(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8)))
						}
					}
				}
				printf("%s:%s%s%s\n", r(2), r(6), r(7), r(8))
			})
		}
	}
	
	/**
	 * normalize recordset
	 * @param line recordset
	 * @return true: continuous; false: not continuous
	 */
	private def _normalize(line:Array[String], skip:SkipState):Boolean = {
		// 町域部分について「以下に掲載がない」場合は、空欄
		if (line(5) == "ｲｶﾆｹｲｻｲｶﾞﾅｲﾊﾞｱｲ" || line(8) == "以下に掲載がない場合") {
			line(5) = ""
			line(8) = ""
		}
		// 町域部分について「の次に番地がくる」場合は、空欄
		if (line(5).contains("ﾉﾂｷﾞﾆﾊﾞﾝﾁｶﾞｸﾙﾊﾞｱｲ") || line(8).contains("の次に番地がくる場合")) {
			line(5) = ""
			line(8) = ""
		}
		// 町域部分について「市・町・村一円」の場合は、末尾を削る
		if (line(5).endsWith("ｲﾁｴﾝ") || line(8).endsWith("一円")) {
			line(5) = line(5).stripSuffix("ｲﾁｴﾝ")
			line(8) = line(8).stripSuffix("一円")
		}
		// 町域部分について「その他」の部分は、削る
		if (line(5).contains("(ｿﾉﾀ)") || line(8).contains("その他")) {
			line(5) = line(5).replaceAll("\\(?ｿﾉﾀ\\)?\\s*$", "")
			line(8) = line(8).replaceAll("[\\(（]?その他[\\)）]?\\s*$", "")
		}
		//　前の行からのスキップ条件をみる
		if (!skip.getFlag()) {
			// 両方の括弧がある場合は調整を行う。
			if ((line(5).contains("(") && line(5).contains(")")) || (line(8).contains("(") && line(8).contains(")"))) {
				// @todo: レコードの分割については別途検討
				// 現状では、ひとまずかっこを外して展開。
				line(5) = line(5).replaceAll("[\\(\\)（）]", "")
				line(8) = line(8).replaceAll("[\\(\\)（）]", "")
				
			// 括弧の開始があって、括弧終了がない場合は、括弧以下を消して次の行をスキップ。
			} else if (line(5).contains("(")  || line(8).contains("(")) {
				line(5) = line(5).replaceAll("\\(.+", "")
				line(8) = line(8).replaceAll("[\\(（].+", "")
				skip.setFlag(true)
			}
		// スキップの場合で閉じるタグがあった場合、処理を戻す
		} else if (line(5).contains(")") || line(8).contains(")")) {
			skip.setFlag(false)
			return false
		}
		return true
	}

	/**
	 * using method like C#
	 * @param in input stream
	 * @param out output stream
	 */
	def using[A <% { def close():Unit }](s:A)(f:A => Any) {
		try {
			f(s)
		} finally {
			s.close()
		}
	}
	
	/**
	 * load properties file
	 * @param filename
	 */
	def loadProperties(filename:String):Properties = {
		val stream = new BufferedInputStream(new FileInputStream(filename))
		val p = new Properties()
		p.load(stream)
		return p
	}
}

/**
 * スキップ状態を保持するクラス
 */
class SkipState {
	var flag:Boolean = false
	def setFlag(newflag:Boolean) {flag = newflag}
	def getFlag() = flag
}