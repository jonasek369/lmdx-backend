import io.ktor.client.HttpClient
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.coroutines.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.statements.api.ExposedBlob
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import java.awt.Image
import java.awt.image.BufferedImage
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.sql.Connection
import java.util.concurrent.Executors
import javax.imageio.ImageIO

@Serializable
data class MangaTags(
    val id: String,
    val type: String,
    val attributes: JsonObject,
    val relationships: JsonArray
)

@Serializable
data class MangaAttributes(
    val title: JsonObject,
    val altTitles: JsonArray,
    val description: JsonObject,
    val isLocked: Boolean,
    val links: JsonObject,
    val originalLanguage: String,
    val lastVolume: String?,
    val lastChapter: String?,
    val publicationDemographic: String?,
    val status: String,
    val year: Int,
    val contentRating: String,
    val tags: List<MangaTags>,
    val state: String,
    val chapterNumbersResetOnNewVolume: Boolean,
    val createdAt: String,
    val updatedAt: String,
    val version: Int,
    val availableTranslatedLanguages: JsonArray,
    val latestUploadedChapter: String?
)

@Serializable
data class Manga(
    val id: String,
    val type: String,
    val attributes: MangaAttributes,
    val relationships: JsonArray
)

data class InfoObject(
    val identifier: String,
    val name: String,
    val description: String?,
    val cover: ByteArray?,
    val smallCover: ByteArray?,
    val mangaFormat: String?,
    val mangaGenre: String?,
    val contentRating: String?
)

object Info : Table() {
    val identifier: Column<String> = varchar("identifier", 36).uniqueIndex()
    val name: Column<String> = varchar("name", 256)
    val description: Column<String?> = varchar("description", 2048).nullable()
    val cover: Column<ExposedBlob?> = blob("cover").nullable()
    val smallCover: Column<ExposedBlob?> = blob("small_cover").nullable()
    val mangaFormat: Column<String?> = varchar("manga_format", 32).nullable()
    val mangaGenre: Column<String?> = varchar("manga_genre", 256).nullable()
    val contentRating: Column<String?> = varchar("content_rating", 32).nullable()
}

object Mangas : Table() {
    val identifier: Column<String> = varchar("identifier", 36)
    val page: Column<Int> = integer("page")
    val data: Column<ExposedBlob> = blob("data")
    val infoId: Column<String?> = (varchar("info_id", 36) references Info.identifier).nullable()
}

object Records: Table() {
    val identifier = varchar("identifier", 36)
    val pages = integer("pages")
}


class MangaDexConnection(mangaDb: MangaDatabase? = null){
    private val httpClient = HttpClient(CIO) {
        install(ContentNegotiation) {
            json(Json {
                prettyPrint = true
                isLenient = true
            })
        }

    }
    private val API_URL = "https://api.mangadex.org";
    private val database = mangaDb

    private fun getDefaultRequestParameters(): JsonElement {
        return Json.parseToJsonElement("""{"contentRating": ["safe", "suggestive", "erotica"]}""")
    }

    fun searchManga(mangaName: String, includeTagIds: List<String> = listOf(), excludeTagIds: List<String> = listOf()): List<Manga>?{
        return runBlocking {
            val defaultParams = getDefaultRequestParameters().jsonObject.toMutableMap();

            defaultParams["title"] = JsonPrimitive(mangaName);
            defaultParams["includedTags[]"] = Json.encodeToJsonElement(includeTagIds)
            defaultParams["excludedTags[]"] = Json.encodeToJsonElement(excludeTagIds)


            val response: HttpResponse = httpClient.get("$API_URL/manga") {
                contentType(ContentType.Application.Json)
                setBody(defaultParams)
            }


            val responseJson = Json.parseToJsonElement(response.body()).jsonObject["data"] ?: return@runBlocking null
            val mangas = Json.decodeFromJsonElement<List<Manga>>(responseJson);

            return@runBlocking mangas
        }
    }

    fun downloadMangaPages(chapterIdentifier: String, pagesInDatabase: List<Int> = listOf(), silent: Boolean = true, rateLimitCallback: ((Float) -> Boolean)? = null): List<Pair<Int, ByteArray>>?{
        return runBlocking {
            val metadata = httpClient.get("$API_URL/at-home/server/$chapterIdentifier")
            val remainingRequests = metadata.headers["X-RateLimit-Remaining"]?.toInt()?: 1 // This should always be supplied by the api
            val retryAfter = metadata.headers["X-RateLimit-Retry-After"]?.toFloat()?: 0 // <--|
            if (remainingRequests <= 0) {
                println("Rate limit reached")
                if(rateLimitCallback != null && rateLimitCallback.invoke(retryAfter as Float)){
                    return@runBlocking null
                }
            }
            val metadataJson = Json.parseToJsonElement(metadata.body())
            val hash = metadataJson.jsonObject["chapter"]?.jsonObject?.get("hash")!!.toString().trim('"')
            val baseUrl = metadataJson.jsonObject["baseUrl"]!!.toString().trim('"')
            val pages = metadataJson.jsonObject["chapter"]?.jsonObject?.get("data")?.jsonArray?.size!!
            database?.let {
                database.setRecord(chapterIdentifier, pages)
            }

            val executor = Executors.newFixedThreadPool(Math.min(pages, Runtime.getRuntime().availableProcessors() + 4))
            val deferreds = mutableListOf<Deferred<Pair<Int, ByteArray>>>()

            suspend fun downloadPage(pageCount: Int, pageDigest: String): Pair<Int, ByteArray>{
                val page = httpClient.get("$baseUrl/data/$hash/${pageDigest.trim('"')}")
                println("Getting $pageDigest")
                return Pair(pageCount+1, page.readBytes());
            }

            val downloadedPages = mutableListOf<Pair<Int, ByteArray>>()

            for ((pageCount, pageDigest) in metadataJson.jsonObject["chapter"]?.jsonObject?.get("data")?.jsonArray?.withIndex()
                ?: emptyList()) {
                if ((pageCount + 1) !in pagesInDatabase /*&& (generate() ?: true)*/) {
                    val deferred  = CoroutineScope(Dispatchers.IO).async { downloadPage(pageCount, pageDigest.toString()) }
                    deferreds.add(deferred)
                }
            }

            for (deferred in deferreds) {
                try {
                    val (pageCount, pageContent) = deferred.await()
                    downloadedPages.add(pageCount to pageContent)
                    /*if (pageDownloadCb != null) {
                        pageDownloadCb(muuid, pageCount to pageContent, pages)
                    }*/
                } catch (e: Exception) {
                    println("An error occurred: $e")
                }
            }

            executor.shutdown()


            // sort them by
            downloadedPages.sortBy { it.first }

            return@runBlocking downloadedPages
        }
    }

    private fun resizeImage(imageData: ByteArray, imageFormat: String,targetWidth: Int, targetHeight: Int): ByteArray {
        // Convert ByteArray to BufferedImage
        val inputStream = ByteArrayInputStream(imageData)
        val originalImage: BufferedImage = ImageIO.read(inputStream)

        // Create a new BufferedImage with desired dimensions
        val resizedImage = originalImage.getScaledInstance(targetWidth, targetHeight, Image.SCALE_SMOOTH)
        val bufferedResizedImage = BufferedImage(targetWidth, targetHeight, BufferedImage.TYPE_INT_RGB)
        bufferedResizedImage.graphics.drawImage(resizedImage, 0, 0, null)

        // Convert BufferedImage to ByteArray
        val outputStream = ByteArrayOutputStream()
        ImageIO.write(bufferedResizedImage, imageFormat, outputStream)
        return outputStream.toByteArray()
    }

    fun getMangaInfo(mangaIdentifier: String): InfoObject{
        return runBlocking {
            val defaultParams = getDefaultRequestParameters().jsonObject.toMutableMap();

            val response = httpClient.get("$API_URL/manga/$mangaIdentifier?includes%5B%5D=cover_art") {
                contentType(ContentType.Application.Json)
                setBody(defaultParams)
            }
            val mangaInfoNonagr = Json.parseToJsonElement(response.body())

            val mangaFormat: MutableList<String> = mutableListOf()
            val mangaGenre: MutableList<String> = mutableListOf()
            var coverUrl: String? = null
            val contentRating = mangaInfoNonagr.jsonObject["data"]?.jsonObject?.get("attributes")?.jsonObject?.get("contentRating").toString().trim('"')

            var image: ByteArray? = null
            var smallImage: ByteArray? = null

            for((index, relationship) in mangaInfoNonagr.jsonObject["data"]?.jsonObject?.get("relationships")?.jsonArray?.withIndex()?: emptyList()){
                if(relationship.jsonObject["type"].toString().trim('"') == "cover_art"){
                    val fileName = mangaInfoNonagr.jsonObject["data"]?.jsonObject?.get("relationships")?.jsonArray?.get(index)?.jsonObject?.get("attributes")?.jsonObject?.get("fileName").toString().trim('"')
                    coverUrl = "https://mangadex.org/covers/$mangaIdentifier/$fileName"
                }
            }

            for(tag in mangaInfoNonagr.jsonObject["data"]?.jsonObject?.get("attributes")?.jsonObject?.get("tags")?.jsonArray?: emptyList()){
                if (tag.jsonObject["attributes"]?.jsonObject?.get("group").toString().trim('"') == "format"){
                    mangaFormat += tag.jsonObject["attributes"]?.jsonObject?.get("name")?.jsonObject?.get("en").toString().trim('"')
                }
                if (tag.jsonObject["attributes"]?.jsonObject?.get("group").toString().trim('"') == "genre"){
                    mangaGenre += tag.jsonObject["attributes"]?.jsonObject?.get("name")?.jsonObject?.get("en").toString().trim('"')
                }
            }

            if(coverUrl != null) {
                val coverArtResponse = httpClient.get(coverUrl)
                val coverArt = coverArtResponse.readBytes()
                image = coverArt.copyOf()
                smallImage = resizeImage(coverArt.copyOf(), "jpg",51, 80)
            }

            val title = if (mangaInfoNonagr.jsonObject["data"]?.jsonObject?.get("attributes")?.jsonObject?.get("title")?.jsonObject?.get("en") == null){
                ((mangaInfoNonagr.jsonObject["data"]?.jsonObject?.get("attributes")?.jsonObject?.get("title")?.jsonObject) as Map<*, *>).values.toList()[0]?.toString()
            } else{
                mangaInfoNonagr.jsonObject["data"]?.jsonObject?.get("attributes")?.jsonObject?.get("title")?.jsonObject?.get("en").toString()
            }?: ""


            return@runBlocking InfoObject(
                identifier =mangaIdentifier,
                name = title,
                description = mangaInfoNonagr.jsonObject["data"]?.jsonObject?.get("attributes")?.jsonObject?.get("description")?.jsonObject?.get("en")
                    .toString(),
                cover = image,
                smallCover = smallImage,
                mangaFormat = mangaFormat.joinToString("|"),
                mangaGenre = mangaGenre.joinToString("|"),
                contentRating = contentRating
            )
        }
    }

    fun getChapterList(mangaIdentifier: String, language: String): List<JsonElement>?{
        val chapters: List<JsonElement> = listOf();
        val offset = 0
        val totalChapters = null

        val defaultParams = getDefaultRequestParameters().jsonObject.toMutableMap();
        while(true){
            if((totalChapters != null) && (totalChapters <= offset)){
                break
            }
            defaultParams["manga"] = JsonPrimitive(mangaIdentifier)
            defaultParams["limit"] = JsonPrimitive(100)
            defaultParams["offset"] = JsonPrimitive(offset)
            defaultParams["translatedLanguage[]"] = JsonPrimitive(language)








            // TODO: FINISH (Github/lmdx Line:840:app.py)
        }
    }
}


class MangaDatabase{
    private val dbConnection = Database.connect("jdbc:sqlite:manga.db", "org.sqlite.JDBC")

    init {
        TransactionManager.manager.defaultIsolationLevel = Connection.TRANSACTION_SERIALIZABLE
        transaction (dbConnection) {
            SchemaUtils.create(Info)
            SchemaUtils.create(Mangas)
            SchemaUtils.create(Records)
            exec("CREATE INDEX IF NOT EXISTS idx_info_id ON mangas(info_id)")
            exec("CREATE INDEX IF NOT EXISTS idx_identifier_page ON mangas (identifier, page);")
            commit()
        }
    }

    fun savePages(chapterIdentifier: String, downloadedPage: List<Pair<Int, ByteArray>>){
        transaction {
            for(manga: Pair<Int, ByteArray> in downloadedPage){
                Mangas.insert {
                    it[identifier] = chapterIdentifier
                    it[page] = manga.first
                    it[data] = ExposedBlob(manga.second)
                    it[infoId] = null
                }
            }
            commit()
        }
    }

    fun setRecord(chapterIdentifier: String, pagesCount: Int) {
        transaction {
            Records.insert {
                it[identifier] = chapterIdentifier
                it[pages] = pagesCount
            }
            commit()
        }
    }

    fun getChapterPages(chapterIdentifier: String): List<Int>{
        val pagesInDb = mutableListOf<Int>()
        transaction {
            val pages = Mangas.select(Mangas.page).where { Mangas.identifier eq chapterIdentifier }.mapTo(pagesInDb) { it[Mangas.page] }
        }
        return pagesInDb
    }

    fun setInfo(infoObject: InfoObject){
        transaction {
            Info.insert {
                it[identifier] = infoObject.identifier
                it[name] = infoObject.name
                it[description] = infoObject.description
                it[cover] = infoObject.cover?.let { it1 -> ExposedBlob(it1) }
                it[smallCover] = infoObject.smallCover?.let { it1 -> ExposedBlob(it1) }
                it[mangaFormat] = infoObject.mangaFormat
                it[mangaGenre] = infoObject.mangaGenre
                it[contentRating] = infoObject.contentRating
            }
            commit()
        }
    }
}


fun main() {
    val database = MangaDatabase()
    val connection = MangaDexConnection(database)
}


