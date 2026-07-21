package com.tibiabot

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.tibiabot.tibiadata.TibiaDataClient
import com.tibiabot.tibiadata.response.{CharacterResponse, GuildResponse, BoostedResponse, CreatureResponse, Members, HighscoresResponse}
import com.tibiabot.scheduler.ServerSaveSchedule
import com.typesafe.scalalogging.StrictLogging
import net.dv8tion.jda.api.entities.channel.concrete.TextChannel
import net.dv8tion.jda.api.entities.{Guild, MessageEmbed}
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent
import net.dv8tion.jda.api.interactions.components.buttons._
import net.dv8tion.jda.api.{EmbedBuilder, Permission}
import net.dv8tion.jda.api.entities.User
import net.dv8tion.jda.api.entities.Role
import net.dv8tion.jda.api.entities.channel.attribute.IPermissionContainer
import net.dv8tion.jda.api.entities.emoji.Emoji
import net.dv8tion.jda.api.entities.Message
import net.dv8tion.jda.api.utils.TimeFormat

import java.awt.Color
import java.time.{Instant, ZonedDateTime}
import scala.collection.immutable.ListMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import scala.util.Random
import scala.concurrent.Await
import com.tibiabot.presentation.Embeds.BrandColor

object BotApp extends App with StrictLogging {

  // Domain model extracted to com.tibiabot.domain. Aliased here (type + companion
  // val) so every existing reference — bare within BotApp and BotApp.X elsewhere —
  // resolves unchanged. Compile-only: no behaviour change.
  type Worlds = domain.Worlds; val Worlds = domain.Worlds
  type Discords = domain.Discords; val Discords = domain.Discords
  type Players = domain.Players; val Players = domain.Players
  type Guilds = domain.Guilds; val Guilds = domain.Guilds
  type BoostedCache = domain.BoostedCache; val BoostedCache = domain.BoostedCache
  type PlayerCache = domain.PlayerCache; val PlayerCache = domain.PlayerCache
  type DeathsCache = domain.DeathsCache; val DeathsCache = domain.DeathsCache
  type LevelsCache = domain.LevelsCache; val LevelsCache = domain.LevelsCache
  type ListCache = domain.ListCache; val ListCache = domain.ListCache
  type SatchelStamp = domain.SatchelStamp; val SatchelStamp = domain.SatchelStamp
  type BoostedStamp = domain.BoostedStamp; val BoostedStamp = domain.BoostedStamp
  type DeathScreenshot = domain.DeathScreenshot; val DeathScreenshot = domain.DeathScreenshot
  type CustomSort = domain.CustomSort; val CustomSort = domain.CustomSort
  type BossEntry = domain.BossEntry; val BossEntry = domain.BossEntry

  // Core hunted/allied/world state, read every cycle by the per-world streams and
  // written by command threads. Declared early (before tibiaDataClient below,
  // which needs it) since object fields initialise top-to-bottom.
  val streamState = new state.StreamState

  implicit private val actorSystem: ActorSystem = ActorSystem()
  implicit private val ex: ExecutionContextExecutor = actorSystem.dispatcher
  private val tibiaDataClient: tibiadata.TibiaApi =
    new tibiadata.CachingTibiaApi(new TibiaDataClient(streamState), persistence.RedisCacheProvider.cache,
      Config.Cache.boostedTtl, Config.Cache.highscoresTtl)(scala.concurrent.ExecutionContext.global)
  private val connectionProvider: persistence.ConnectionProvider =
    new persistence.JdbcConnectionProvider(Config.postgresHost, Config.postgresPassword)
  private val schemaInitializer = new persistence.SchemaInitializer(connectionProvider)
  private val boostedRepository: persistence.BoostedRepository =
    new persistence.jdbc.JdbcBoostedRepository(connectionProvider)
  private lazy val wikiClient: wiki.WikiClient = new wiki.FandomWikiClient()
  private val galthenRepository: persistence.GalthenRepository =
    new persistence.jdbc.JdbcGalthenRepository(connectionProvider)
  private val deathScreenshotRepository: persistence.DeathScreenshotRepository =
    new persistence.jdbc.JdbcDeathScreenshotRepository(connectionProvider)
  private val cacheRepository: persistence.CacheRepository =
    new persistence.jdbc.JdbcCacheRepository(connectionProvider)
  private val activityRepository: persistence.ActivityRepository =
    new persistence.jdbc.JdbcActivityRepository(connectionProvider)
  private val huntedAlliedRepository: persistence.HuntedAlliedRepository =
    new persistence.jdbc.JdbcHuntedAlliedRepository(connectionProvider)
  private val customSortRepository: persistence.CustomSortRepository =
    new persistence.jdbc.JdbcCustomSortRepository(connectionProvider)
  private val worldConfigRepository: persistence.WorldConfigRepository =
    new persistence.jdbc.JdbcWorldConfigRepository(connectionProvider, Config.mergedWorlds)
  private val discordConfigRepository: persistence.DiscordConfigRepository =
    new persistence.jdbc.JdbcDiscordConfigRepository(connectionProvider)

  // Let the games begin
  logger.info("Starting up")

  val jda = app.Bootstrap.buildReadyJda(Config.token, new BotListener())
  logger.info("JDA ready")

  // single read-side seam over JDA (guild/user lookups, identity, presence)
  val discordGateway: discord.DiscordGateway = new discord.JdaDiscordGateway(jda)

  // get the discord servers the bot is in
  private val guilds: List[Guild] = discordGateway.guilds

  // per-world stream lifecycle
  private val streamSupervisor = new app.StreamSupervisor

  // Galthen's Satchel cooldown tracking
  val galthenService = new galthen.GalthenService(galthenRepository, connectionProvider, discordGateway)

  // Per-user boosted boss/creature notification subscriptions
  val boostedService = new boosted.BoostedService(connectionProvider, boostedRepository, cacheRepository, tibiaDataClient, () => boostedBossesList)

  // Per-guild hunted/allied player and guild list CRUD
  val huntedAlliedService = new hunted.HuntedAlliedService(
    huntedAlliedRepository, activityRepository, cacheRepository, streamState, tibiaDataClient,
    discordRetrieveConfig _, worldConfig _, checkConfigDatabase _
  )

  // Per-guild custom online-list tag categories (/neutral tag ...)
  val customSortService = new customsort.CustomSortService(
    customSortRepository, streamState, tibiaDataClient, huntedAlliedService.fetchPlayerSummary _,
    discordRetrieveConfig _, checkConfigDatabase _
  )

  // get bot userID (used to stamp automated enemy detection messages)
  val botUser = discordGateway.selfUserId
  // the application owner = the bot creator (used to gate /admin)
  val botOwner: String = discordGateway.applicationOwnerId

  // Bot-creator-only /admin operations (needs botUser, defined above)
  val adminService = new admin.AdminService(
    discordGateway,
    botUser,
    discordRetrieveConfig _,
    () => { dreamScar = fetchDreamScarBosses().map(e => e.world -> e.boss).toMap }
  )

  // streamState is declared above (before tibiaDataClient). BotApp delegates so
  // existing call sites (BotApp.activityData / modifyActivityData / ...) are unchanged.
  def activityData: Map[String, List[PlayerCache]] = streamState.activityData
  def huntedPlayersData: Map[String, List[Players]] = streamState.huntedPlayersData
  def alliedPlayersData: Map[String, List[Players]] = streamState.alliedPlayersData
  def huntedGuildsData: Map[String, List[Guilds]] = streamState.huntedGuildsData
  def alliedGuildsData: Map[String, List[Guilds]] = streamState.alliedGuildsData
  def customSortData: Map[String, List[CustomSort]] = streamState.customSortData
  def discordsData: Map[String, List[Discords]] = streamState.discordsData
  def worldsData: Map[String, List[Worlds]] = streamState.worldsData
  def activityCommandBlocker: Map[String, Boolean] = streamState.activityCommandBlocker
  def characterCache: Map[String, ZonedDateTime] = streamState.characterCache
  def modifyActivityData(f: Map[String, List[PlayerCache]] => Map[String, List[PlayerCache]]): Unit =
    streamState.modifyActivityData(f)
  def modifyHuntedPlayersData(f: Map[String, List[Players]] => Map[String, List[Players]]): Unit =
    streamState.modifyHuntedPlayersData(f)
  def modifyAlliedPlayersData(f: Map[String, List[Players]] => Map[String, List[Players]]): Unit =
    streamState.modifyAlliedPlayersData(f)
  def modifyHuntedGuildsData(f: Map[String, List[Guilds]] => Map[String, List[Guilds]]): Unit =
    streamState.modifyHuntedGuildsData(f)
  def modifyAlliedGuildsData(f: Map[String, List[Guilds]] => Map[String, List[Guilds]]): Unit =
    streamState.modifyAlliedGuildsData(f)
  def modifyCustomSortData(f: Map[String, List[CustomSort]] => Map[String, List[CustomSort]]): Unit =
    streamState.modifyCustomSortData(f)
  def modifyDiscordsData(f: Map[String, List[Discords]] => Map[String, List[Discords]]): Unit =
    streamState.modifyDiscordsData(f)
  def modifyWorldsData(f: Map[String, List[Worlds]] => Map[String, List[Worlds]]): Unit =
    streamState.modifyWorldsData(f)
  def modifyActivityCommandBlocker(f: Map[String, Boolean] => Map[String, Boolean]): Unit =
    streamState.modifyActivityCommandBlocker(f)
  def modifyCharacterCache(f: Map[String, ZonedDateTime] => Map[String, ZonedDateTime]): Unit =
    streamState.modifyCharacterCache(f)

  // R1: warm the Date-header character cache from the last Redis snapshot so a
  // restart doesn't re-baseline ~8000 characters against the rate-limited API,
  // then snapshot it every 60s. Whole-map snapshot keeps the per-character hot
  // path off Redis entirely; no-op + empty load when Redis is disabled.
  private val charCachePersistence =
    new persistence.CharacterCachePersistence(persistence.RedisCacheProvider.cache, Config.Cache.characterSnapshotTtl)(ex)
  charCachePersistence.load().foreach { loaded =>
    if (loaded.nonEmpty) {
      modifyCharacterCache(existing => loaded ++ existing) // existing (fresher) entries win
      logger.info(s"Warmed character cache from Redis snapshot: ${loaded.size} entries")
    }
  }
  private val snapshotInterval = Config.Cache.characterSnapshotInterval
  actorSystem.scheduler.scheduleWithFixedDelay(snapshotInterval, snapshotInterval)(() => { charCachePersistence.save(characterCache); () })(ex)

  val worlds: List[String] = Config.worldList

  // Per-guild channel/role setup lifecycle (extraction of the channel ops from
  // BotApp is in progress; currently holds discordJoin/discordLeave and
  // createChannels/`setup`). State mutation for join/leave stays in BotApp via
  // the forgetGuild callback; createChannels reads/writes streamState directly.
  val channelService = new setup.ChannelService(
    streamSupervisor,
    schemaInitializer,
    worldConfigRepository,
    discordConfigRepository,
    streamState,
    boostedService,
    botUser,
    startBot = (guild, world) => startBot(guild, world),
    serverSaveExtraEmbeds = world => serverSaveExtraEmbeds(world),
    forgetGuild = guildId => {
      if (worldsData.contains(guildId)) modifyWorldsData(_ - guildId)
      val updatedDiscordsData = discordsData.map { case (world, discordsList) =>
        if (discordsList.exists(_.id == guildId)) world -> discordsList.filterNot(_.id == guildId)
        else world -> discordsList
      }
      if (updatedDiscordsData != discordsData) modifyDiscordsData(_ => updatedDiscordsData)
    },
    sharedConfigGuilds = Set("912739993015947324", "1176279097001918516", "1224670957466161234")
  )

  // Dream Courts boss rotation extracted to domain.time.DreamScarCycle.
  // dreamScar/dromeTime are written by the scheduler thread (and dreamScar also by
  // the /admin resync thread) but read every cycle by the per-world streams — so
  // they need @volatile for the same cross-thread visibility reason as the state
  // below; without it a stream can keep reading a stale boss/cycle after a shift.
  val bossCycle = domain.time.DreamScarCycle.bossCycle
  val indexOfBoss: Map[String, Int] = domain.time.DreamScarCycle.indexOfBoss
  @volatile var dreamScar: Map[String, String] = fetchDreamScarBosses().map(e => e.world -> e.boss).toMap
  @volatile var dreamScarLastCheck: String = System.currentTimeMillis().toString
  @volatile var dromeTime = domain.time.DromeCycle.initial // 27 May 2026 server save - increment 2 weeks from here

  // Boosted Boss
  val boostedBosses: Future[Either[String, BoostedResponse]] = tibiaDataClient.getBoostedBoss()
  val bossFuture: Future[List[String]] = boostedBosses.map {
    case Right(boostedResponse) =>
      val boostedBoss = boostedResponse.boostable_bosses.boostable_boss_list
      val boostedBossList = boostedBoss.map(_.name.toLowerCase).toList
      boostedBossList
    case Left(errorMessage) =>
      List.empty[String]
  }

  // Combine both futures and send the message
  private var updateOnOdd = 0

  val bossesFutures: Future[List[String]] = for {
    bosses <- bossFuture
  } yield bosses

  val boostedBossesList: List[String] = Await.result(bossesFutures, 10.seconds)

  // Slash command schemas live in commands.CommandSchemas
  lazy val commands = com.tibiabot.commands.CommandSchemas.commands

  // create the deaths/levels cache db
  createCacheDatabase()

  // initialize the database
  guilds.foreach{g =>
    if (g.getIdLong == 867319250708463628L || g.getIdLong == 1082484147492237515L) { // Violent Bot Discords
      val adminCommands = com.tibiabot.commands.CommandSchemas.adminCommands
      g.updateCommands().addCommands(adminCommands.asJava).complete()
    } else {
      // update the commands
      g.updateCommands().addCommands(commands.asJava).complete()
    }
  }

  // Start all world streams
  // Written once on the startup thread (after all world streams are launched) and
  // read on JDA event threads in BotListener — @volatile so a command thread can't
  // cache the initial false and reject every slash command as "still starting up".
  @volatile var startUpComplete = false
  val startTime = Instant.now()
  // update Drome Timer to the latest cycle
  if (dromeTime.isBefore(startTime)) {
    advanceDromeTime(startTime)
  }
  startBot(None, None) // guild: Option[Guild], world: Option[String]

  // run the scheduler to clean cache and update dashboard every hour.
  // scheduleWithFixedDelay (not the deprecated schedule) so a slow cycle — this
  // body makes blocking API calls at server save — can't pile up behind itself.
  actorSystem.scheduler.scheduleWithFixedDelay(60.seconds, 30.seconds)(() => {
    // set activity status
    // only do this every second cycle
    if (updateOnOdd >= 10) {
      try {
        val randomActivity = List(
          "number go up",
          "Tibia players die",
          "some kid red skull",
          "UE combos slap",
          "another 50k spent on twist"
        )
        val randomActivityFromList = Random.shuffle(randomActivity).headOption.getOrElse("people press buttons")
        discordGateway.setWatchingActivity(randomActivityFromList)
      } catch {
        case ex: Throwable => logger.warn("Failed to update the bot's status counts", ex)
      }
      removeDeathsCache(ZonedDateTime.now())
      removeLevelsCache(ZonedDateTime.now())
      cleanHuntedList()
      galthenService.cleanExpired()
      cleanOnlineListCache(30)
      updateOnOdd = 0 // Toggle the flag
    } else {
      updateOnOdd += 1
    }
    // Updating boosted creature/boss at server save
    val currentTime = ZonedDateTime.now(ZoneId.of("Europe/Berlin")).toLocalTime()
    if (ServerSaveSchedule.isServerSaveWindow(currentTime)) {
      try{
        val now = System.currentTimeMillis()
        if (now - dreamScarLastCheck.toLong > 60L * 60 * 1000) {
          dreamScarLastCheck = now.toString
          dreamScar = shiftAllBossesUp(dreamScar)
        }
        if (dromeTime.isBefore(Instant.now())) {
          advanceDromeTime(Instant.now())
        }
      }
      catch {
        case ex: Throwable => logger.warn("Failed to get Dream Boss info from wiki", ex)
      }
      try {
        boostedService.boostedMessages().map { boostedBossAndCreature =>
          val currentBoss = boostedBossAndCreature.boss
          val currentCreature = boostedBossAndCreature.creature
          val bossChanged = boostedBossAndCreature.bossChanged
          val creatureChanged = boostedBossAndCreature.creatureChanged

          // Boosted Boss
          val boostedBoss: Future[Either[String, BoostedResponse]] = tibiaDataClient.getBoostedBoss()
          val bossEmbedFuture: Future[(MessageEmbed, Boolean, String)] = boostedBoss.map {
            case Right(boostedResponse) =>
              val boostedBoss = boostedResponse.boostable_bosses.boosted.name
              if (boostedBoss.toLowerCase != currentBoss.toLowerCase) {
                boostedService.boostedMonsterUpdate(boostedBoss, "", "1", "")
              }
              (
                presentation.BoostedEmbeds.create(creatureImageUrl(boostedBoss),s"The boosted boss today is:\n### ${Config.indentEmoji}${Config.archfoeEmoji} **[$boostedBoss](${creatureWikiUrl(boostedBoss)})**"),
                boostedBoss.toLowerCase != currentBoss.toLowerCase && currentBoss.toLowerCase != "none",
                boostedBoss
              )

            case Left(errorMessage) =>
              throw new Exception(s"Failed to load boosted boss.")
          }

          // Boosted Creature
          val boostedCreature: Future[Either[String, CreatureResponse]] = tibiaDataClient.getBoostedCreature()
          val creatureEmbedFuture: Future[(MessageEmbed, Boolean, String)] = boostedCreature.map {
            case Right(creatureResponse) =>
              val boostedCreature = creatureResponse.creatures.boosted.name
              if (boostedCreature.toLowerCase != currentCreature.toLowerCase) {
                boostedService.boostedMonsterUpdate("", boostedCreature, "", "1")
              }
              (
                presentation.BoostedEmbeds.create(creatureImageUrl(boostedCreature),s"The boosted creature today is:\n### ${Config.indentEmoji}${Config.levelUpEmoji} **[$boostedCreature](${creatureWikiUrl(boostedCreature)})**"),
                boostedCreature.toLowerCase != currentCreature.toLowerCase && currentCreature.toLowerCase != "none",
                boostedCreature
              )

            case Left(errorMessage) =>
              throw new Exception(s"Failed to load boosted boss.")
          }

          // Combine both futures and send the message
          val combinedFutures: Future[List[(MessageEmbed, Boolean, String)]] = for {
            bossEmbed <- bossEmbedFuture
            creatureEmbed <- creatureEmbedFuture
          } yield List(bossEmbed, creatureEmbed)

          combinedFutures.map { boostedInfoList =>
            if (bossChanged == "1" && creatureChanged == "1") {
              boostedService.boostedMonsterUpdate("", "", "0", "0")
              // Do something if at least one of the embeds changed
              val embeds: List[MessageEmbed] = boostedInfoList.map { case (embed, _, _) => embed }.toList
              val notificationsList: List[BoostedStamp] = boostedService.boostedAll()
              notificationsList.foreach { entry =>
                var matchedNotification = false
                boostedInfoList.foreach { case (_, _, boostedName) =>
                  if (boostedName.toLowerCase == entry.boostedName.toLowerCase || entry.boostedName.toLowerCase == "all") {
                    matchedNotification = true
                  }
                }
                if (matchedNotification) {
                  val user: User = discordGateway.retrieveUser(entry.user)
                  if (user != null) {
                    try {
                      user.openPrivateChannel().queue { privateChannel =>
                        val messageText = s"🔔 ${boostedInfoList.head._3} • ${boostedInfoList.last._3}"
                        privateChannel.sendMessage(messageText).setEmbeds(embeds.asJava).setActionRow(
                          Button.primary("boosted list", " ").withEmoji(Emoji.fromFormatted(Config.letterEmoji))
                        ).queue()
                      }
                    } catch {
                      case ex: Exception => logger.warn(s"Failed to send Boosted notification to user: '${entry.user}'", ex)
                    }
                  }
                }
              }

              discordGateway.guilds.foreach { guild =>
                if (checkConfigDatabase(guild)) {
                  val discordInfo = discordRetrieveConfig(guild)
                  val channelId = if (discordInfo.nonEmpty) discordInfo("boosted_channel") else "0"
                  val lastWorld = if (discordInfo.nonEmpty) discordInfo("last_world") else "Antica"
                  if (channelId != "0") {
                    val boostedChannel = guild.getTextChannelById(channelId)
                    if (boostedChannel != null) {
                      if (boostedChannel.canTalk()) {
                        val boostedMessage = if (discordInfo.nonEmpty) discordInfo("boosted_messageid") else "0"
                        if (boostedMessage != "0") {
                          try {
                            boostedChannel.deleteMessageById(boostedMessage).queue()
                          } catch {
                            case ex: Throwable => logger.warn(s"Failed to get the boosted boss creature message for deletion in Guild ID: '${guild.getId}' Guild Name: '${guild.getName}':", ex)
                          }
                        }

                        val dreamScarDaily =
                          dreamScar
                            .get(lastWorld)
                            .orElse(dreamScar.get("Unknown"))
                            .getOrElse("Unknown")

                        val rashidLocation = ServerSaveSchedule.rashidLocation(ZonedDateTime.now(ZoneId.of("Europe/Berlin")).minusHours(10).getDayOfWeek)
                        val rashidEmbed = new EmbedBuilder()
                        rashidEmbed.setDescription(s"Today Rashid can be found in:\n### ${Config.indentEmoji}${Config.goldEmoji} **[${rashidLocation}](https://tibia.fandom.com/wiki/Rashid)**")
                        rashidEmbed.setThumbnail("https://www.tibiawiki.com.br/wiki/Special:Redirect/file/Rashid.gif")
                        rashidEmbed.setColor(BrandColor)

                        // Drome Timer
                        val now = Instant.now()
                        val dromeShow = ServerSaveSchedule.shouldShowDrome(now, dromeTime)
                        val dromeEmbed = new EmbedBuilder()
                          .setDescription(s"The current Drome cycle will end:\n### ${Config.indentEmoji}${Config.dromeEmoji} ${TimeFormat.RELATIVE.format(dromeTime)}")
                          .setThumbnail("https://www.tibiawiki.com.br/wiki/Special:Redirect/file/Phant.gif")
                          .setColor(BrandColor)

                        val dreamScarEmbed = new EmbedBuilder()
                        dreamScarEmbed.setDescription(s"The Dream Courts boss for **$lastWorld** is:\n### ${Config.indentEmoji}${Config.dreamScarEmoji} **[${dreamScarDaily}](https://tibia.fandom.com/wiki/Dream_Scar/Boss_of_the_Day)**")
                        dreamScarEmbed.setThumbnail(creatureImageUrl(dreamScarDaily))
                        dreamScarEmbed.setColor(BrandColor)

                        val embedsList = if (dromeShow) List(rashidEmbed.build(), dreamScarEmbed.build(), dromeEmbed.build()) else List(rashidEmbed.build(), dreamScarEmbed.build())
                        val addRashidDreamScarEmbeds: List[MessageEmbed] = embeds ++ embedsList

                        boostedChannel.sendMessageEmbeds(addRashidDreamScarEmbeds.asJava)
                          .setActionRow(
                            Button.primary("boosted list", "Server Save Notifications").withEmoji(Emoji.fromFormatted(Config.letterEmoji))
                          )
                          .queue((message: Message) => {
                            //updateBoostedMessage(guild.getId, message.getId)
                            discordUpdateConfig(guild, "", "", "", message.getId, lastWorld)
                          }, (e: Throwable) => {
                            logger.warn(s"Failed to send boosted boss/creature message for Guild ID: '${guild.getId}' Guild Name: '${guild.getName}':", e)
                          })
                      } else {
                        logger.warn(s"Failed to send & delete boosted message for Guild ID: '${guild.getId}' Guild Name: '${guild.getName}': no VIEW/SEND permissions")
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
      catch {
        case ex : Throwable => logger.warn("Failed to update the boosted messages", ex)
      }
    }
  })

  def cleanOnlineListCache(maxAgeMinutes: Long): Unit = {
    val currentTime = ZonedDateTime.now()

    modifyCharacterCache(_.filter {
      case (_, timestamp) =>
        val ageMinutes = timestamp.until(currentTime, java.time.temporal.ChronoUnit.MINUTES)
        ageMinutes <= maxAgeMinutes
    })
  }

  private def startBot(guild: Option[Guild], world: Option[String]): Unit = {

    if (guild.isDefined && world.isDefined) {

      val guildId = guild.get.getId

        // get hunted Players
        val huntedPlayers = playerConfig(guild.get, "hunted_players")
        modifyHuntedPlayersData(_ + (guildId -> huntedPlayers))

        // get allied Players
        val alliedPlayers = playerConfig(guild.get, "allied_players")
        modifyAlliedPlayersData(_ + (guildId -> alliedPlayers))

        // get hunted guilds
        val huntedGuilds = guildConfig(guild.get, "hunted_guilds")
        modifyHuntedGuildsData(_ + (guildId -> huntedGuilds))

        // get allied guilds
        val alliedGuilds = guildConfig(guild.get, "allied_guilds")
        modifyAlliedGuildsData(_ + (guildId -> alliedGuilds))

        // get worlds
        val worldsInfo = worldConfig(guild.get)
        modifyWorldsData(_ + (guildId -> worldsInfo))

        // get tracked activity characters
        val activityInfo = activityConfig(guild.get, "tracked_activity")
        modifyActivityData(_ + (guildId -> activityInfo))

        // get customSort Data
        val customSortInfo = customSortConfig(guild.get, "online_list_categories")
        modifyCustomSortData(_ + (guildId -> customSortInfo))

        // set default activityCommandBlocker state
        modifyActivityCommandBlocker(_ + (guildId -> false))

        val adminChannels = discordRetrieveConfig(guild.get)
        val adminChannelId = if (adminChannels.nonEmpty) adminChannels("admin_channel") else "0"
        val boostedChannelId = if (adminChannels.nonEmpty) adminChannels("boosted_channel") else "0"
        val boostedMessageId = if (adminChannels.nonEmpty) adminChannels("boosted_messageid") else "0"

        worldsInfo.foreach{ w =>
          if (w.name == world.get) {
            val discords = Discords(
              id = guildId,
              adminChannel = adminChannelId,
              boostedChannel = boostedChannelId,
              boostedMessage = boostedMessageId
            )
            modifyDiscordsData(d => d.updated(w.name, discords :: d.getOrElse(w.name, Nil)))
            // Preserves prior behaviour: when the world stream already exists it was
            // left unchanged (the usedBy append was overwritten and never took effect);
            // only an absent world starts a new stream.
            if (!streamSupervisor.contains(world.get)) {
              streamSupervisor.put(world.get, new TibiaBot(world.get).stream.run(), List(discords))
            }
          }
        }
    } else {
      // build guild specific data map
      guilds.foreach{g =>

        val guildId = g.getId

          if (checkConfigDatabase(g)) {
            // get hunted Players
            val huntedPlayers = playerConfig(g, "hunted_players")
            modifyHuntedPlayersData(_ + (guildId -> huntedPlayers))

            // get allied Players
            val alliedPlayers = playerConfig(g, "allied_players")
            modifyAlliedPlayersData(_ + (guildId -> alliedPlayers))

            // get hunted guilds
            val huntedGuilds = guildConfig(g, "hunted_guilds")
            modifyHuntedGuildsData(_ + (guildId -> huntedGuilds))

            // get allied guilds
            val alliedGuilds = guildConfig(g, "allied_guilds")
            modifyAlliedGuildsData(_ + (guildId -> alliedGuilds))

            // get worlds
            val worldsInfo = worldConfig(g)
            modifyWorldsData(_ + (guildId -> worldsInfo))

            // get tracked activity characters
            val activityInfo = activityConfig(g, "tracked_activity")
            modifyActivityData(_ + (guildId -> activityInfo))

            // get customSort Data
            val customSortInfo = customSortConfig(g, "online_list_categories")
            modifyCustomSortData(_ + (guildId -> customSortInfo))

            // set default activityCommandBlocker state
            modifyActivityCommandBlocker(_ + (guildId -> false))

            val adminChannels = discordRetrieveConfig(g)
            val adminChannelId = if (adminChannels.nonEmpty) adminChannels("admin_channel") else "0"
            val boostedChannelId = if (adminChannels.nonEmpty) adminChannels("boosted_channel") else "0"
            val boostedMessageId = if (adminChannels.nonEmpty) adminChannels("boosted_messageid") else "0"

            // populate a new Discords list so i can only run 1 stream per world
            worldsInfo.foreach{ w =>
              val discords = Discords(
                id = guildId,
                adminChannel = adminChannelId,
                boostedChannel = boostedChannelId,
                boostedMessage = boostedMessageId
              )
              modifyDiscordsData(d => d.updated(w.name, discords :: d.getOrElse(w.name, Nil)))
            }
          }
      }
      discordsData.foreach { case (worldName, discordsList) =>
        streamSupervisor.put(worldName, new TibiaBot(worldName).stream.run(), discordsList)
        Thread.sleep(5500) // space each stream out 3 seconds
      }
      startUpComplete = true
    }

    /***
    // check if world parameter has been passed, and convert to a list
    val guildWorlds = world match {
      case Some(worldName) => worldsData.getOrElse(guild.getId, List()).filter(w => w.name == worldName)
      case None => worldsData.getOrElse(guild.getId, List())
    }
    ***/
  }

  private def cleanHuntedList(): Unit =
    cacheRepository.removeExpiredList(ZonedDateTime.now())


  /** The Rashid / Dream Courts / (Drome, when active) server-save embeds for a
   *  world, appended after the boosted embeds in the notifications message.
   *  Reads the live dreamScar map and dromeTime. */
  private def serverSaveExtraEmbeds(world: String): List[MessageEmbed] = {
    val dreamScarDaily =
      dreamScar
        .get(world)
        .orElse(dreamScar.get("Unknown"))
        .getOrElse("Unknown")
    val rashidLocation = ServerSaveSchedule.rashidLocation(ZonedDateTime.now(ZoneId.of("Europe/Berlin")).minusHours(10).getDayOfWeek)
    val rashidEmbed = new EmbedBuilder()
      .setDescription(s"Today Rashid can be found in:\n### ${Config.indentEmoji}${Config.goldEmoji} **[${rashidLocation}](https://tibia.fandom.com/wiki/Rashid)**")
      .setThumbnail("https://www.tibiawiki.com.br/wiki/Special:Redirect/file/Rashid.gif")
      .setColor(BrandColor)
      .build()
    val dreamScarEmbed = new EmbedBuilder()
      .setDescription(s"The Dream Courts boss for **$world** is:\n### ${Config.indentEmoji}${Config.dreamScarEmoji} **[${dreamScarDaily}](https://tibia.fandom.com/wiki/Dream_Scar/Boss_of_the_Day)**")
      .setThumbnail(creatureImageUrl(dreamScarDaily))
      .setColor(BrandColor)
      .build()
    val dromeShow = ServerSaveSchedule.shouldShowDrome(Instant.now(), dromeTime)
    val dromeEmbed = new EmbedBuilder()
      .setDescription(s"The current Drome cycle will end:\n### ${Config.indentEmoji}${Config.dromeEmoji} ${TimeFormat.RELATIVE.format(dromeTime)}")
      .setThumbnail("https://www.tibiawiki.com.br/wiki/Special:Redirect/file/Phant.gif")
      .setColor(BrandColor)
      .build()
    if (dromeShow) List(rashidEmbed, dreamScarEmbed, dromeEmbed) else List(rashidEmbed, dreamScarEmbed)
  }

  /** The role-subscription buttons under the fullbless/notifications embed. */
  private def fullblessRoleButtons: List[Button] = List(
    Button.success("fullbless", " ").withEmoji(Emoji.fromFormatted(Config.inqEmoji)),
    Button.primary("nemesis", " ").withEmoji(Emoji.fromFormatted(Config.bossEmoji)),
    Button.danger("allypk", " ").withEmoji(Emoji.fromFormatted(Config.hazardEmoji)),
    Button.secondary("masslog", " ").withEmoji(Emoji.fromFormatted(Config.masslogEmoji))
  )

  /** The "the bot will poke" role-notification embed for a world. Built by both
   *  /setup (initial post) and /fullbless (edits the existing message). */
  private def fullblessRoleEmbed(world: String, fullblessRoleId: String, nemesisRoleId: String, allyPkRoleId: String, masslogRoleId: String, level: Int): MessageEmbed =
    new EmbedBuilder()
      .setTitle(s":crossed_swords: $world :crossed_swords:", s"https://www.tibia.com/community/?subtopic=worlds&world=$world")
      .setThumbnail("https://raw.githubusercontent.com/Leo32onGIT/tibia-bot-resources/main/Phantasmal_Ooze.gif")
      .setColor(BrandColor)
      .setFooter("Add or remove yourself from the role using the buttons below:")
      .setDescription(s"The bot will poke:\n${Config.inqEmoji}<@&$fullblessRoleId> If an enemy fullblesses and is over level `$level`\n${Config.bossEmoji}<@&$nemesisRoleId> If anyone dies to a rare boss\n${Config.hazardEmoji}<@&$allyPkRoleId> If an ally gets pked\n${Config.masslogEmoji}<@&$masslogRoleId> If enemies masslog on **$world**")
      .build()

  def charUrl(char: String): String = presentation.Urls.charUrl(char)

  def guildUrl(guild: String): String = presentation.Urls.guildUrl(guild)

  def updateBoostedMessage(inputId: String, messageId: String): Unit = {
    modifyDiscordsData(dd => dd.view.mapValues(_.map {
      case discord @ Discords(id, _, _, _) if id == inputId =>
        discord.copy(boostedMessage = messageId)
      case other => other
    }).toMap)
  }

  private def checkConfigDatabase(guild: Guild): Boolean = schemaInitializer.guildDatabaseExists(guild.getId)

  private def createCacheDatabase(): Unit = schemaInitializer.initCache()

  def getDeathsCache(world: String): List[DeathsCache] = cacheRepository.getDeaths(world)

  def addDeathsCache(world: String, name: String, time: String): Unit =
    cacheRepository.addDeath(world, name, time)

  private def removeDeathsCache(time: ZonedDateTime): Unit =
    cacheRepository.removeExpiredDeaths(time)

  def getLevelsCache(world: String): List[LevelsCache] = cacheRepository.getLevels(world)

  def addLevelsCache(world: String, name: String, level: String, vocation: String, lastLogin: String, time: String): Unit =
    cacheRepository.addLevel(world, name, level, vocation, lastLogin, time)

  private def removeLevelsCache(time: ZonedDateTime): Unit =
    cacheRepository.removeExpiredLevels(time)

  private def createConfigDatabase(guild: Guild): Unit = schemaInitializer.initGuild(guild.getId, guild.getName)

  private def playerConfig(guild: Guild, query: String): List[Players] =
    huntedAlliedRepository.getPlayers(guild.getId, query)

  private def guildConfig(guild: Guild, query: String): List[Guilds] =
    huntedAlliedRepository.getGuilds(guild.getId, query)

  private def activityConfig(guild: Guild, query: String): List[PlayerCache] =
    activityRepository.getActivity(guild.getId)

  def discordRetrieveConfig(guild: Guild): Map[String, String] =
    discordConfigRepository.getConfig(guild.getId)

  private def worldConfig(guild: Guild): List[Worlds] =
    worldConfigRepository.listWorlds(guild.getId)

  private def worldCreateConfig(guild: Guild, world: String, alliesChannel: String, enemiesChannel: String, neutralsChannels: String, levelsChannel: String, deathsChannel: String, category: String, fullblessRole: String, nemesisRole: String, allyPkRole: String, masslogRole: String, fullblessChannel: String, nemesisChannel: String, activityChannel: String): Unit =
    worldConfigRepository.createWorld(guild.getId, world, alliesChannel, enemiesChannel, neutralsChannels, levelsChannel, deathsChannel, category, fullblessRole, nemesisRole, allyPkRole, masslogRole, fullblessChannel, nemesisChannel, activityChannel)

  private def discordCreateConfig(guild: Guild, guildName: String, guildOwner: String, adminCategory: String, adminChannel: String, boostedChannel: String, boostedMessageId: String, created: ZonedDateTime): Unit =
    discordConfigRepository.create(guild.getId, guildName, guildOwner, adminCategory, adminChannel, boostedChannel, boostedMessageId, created)

  private def discordUpdateConfig(guild: Guild, adminCategory: String, adminChannel: String, boostedChannel: String, boostedMessage: String, lastWorld: String): Unit =
    discordConfigRepository.update(guild.getId, adminCategory, adminChannel, boostedChannel, boostedMessage, lastWorld)

  def worldRetrieveConfig(guild: Guild, world: String): Map[String, String] =
    worldConfigRepository.retrieveWorld(guild.getId, world)

  /** Generic guarded update for a single per-world setting stored on `Worlds`:
   *  returns an "already set" embed if the value is unchanged or the world
   *  isn't configured (currentValue yields None), otherwise updates the
   *  in-memory cache, persists, posts an admin-log entry, and returns a
   *  "now set" embed. Used by the toggle-shaped world settings (auto-hunt
   *  detection, deaths/levels visibility, exiva list, minimum level).
   *  Settings with additional side effects (e.g. fullbless level, which also
   *  edits a live Discord embed) implement their own, not this helper. */
  private def updateWorldSetting[T](
    guild: Guild,
    world: String,
    newValue: T,
    currentValue: Worlds => Option[T],
    applyValue: (Worlds, T) => Worlds,
    persist: T => Unit,
    alreadySetMessage: String,
    nowSetMessage: String,
    notConfiguredMessage: String,
    adminLogMessage: String,
    adminLogThumbnail: String
  ): MessageEmbed = {
    val embedBuild = new EmbedBuilder()
    embedBuild.setColor(BrandColor)
    val cache = worldsData.getOrElse(guild.getId, List()).filter(w => w.name.toLowerCase() == world.toLowerCase())
    cache.headOption.flatMap(currentValue) match {
      case None =>
        embedBuild.setDescription(notConfiguredMessage)
      case Some(existing) if existing == newValue =>
        embedBuild.setDescription(alreadySetMessage)
      case Some(_) =>
        val modifiedWorlds = worldsData(guild.getId).map { w =>
          if (w.name.toLowerCase() == world.toLowerCase()) applyValue(w, newValue) else w
        }
        modifyWorldsData(_ + (guild.getId -> modifiedWorlds))
        persist(newValue)

        val discordConfig = discordRetrieveConfig(guild)
        val adminChannelId = if (discordConfig.nonEmpty) discordConfig("admin_channel") else ""
        val adminChannel: TextChannel = guild.getTextChannelById(adminChannelId)
        presentation.AdminLog.post(adminChannel, adminLogMessage, adminLogThumbnail)

        embedBuild.setDescription(nowSetMessage)
    }
    embedBuild.build()
  }

  def detectHunted(event: SlashCommandInteractionEvent): MessageEmbed = {
    val options: Map[String, String] = event.getInteraction.getOptions.asScala.map(option => option.getName.toLowerCase() -> option.getAsString.trim()).toMap
    val worldOption: String = options.getOrElse("world", "")
    val settingOption: String = options.getOrElse("option", "")
    val worldFormal = domain.WorldName.formal(worldOption).trim
    val guild = event.getGuild
    val commandUser = event.getUser.getId
    updateWorldSetting[String](
      guild, worldOption, settingOption,
      currentValue = w => Some(w.detectHunteds),
      applyValue = (w, v) => w.copy(detectHunteds = v),
      persist = v => detectHuntedsToDatabase(guild, worldFormal, v),
      alreadySetMessage = s"${Config.noEmoji} **Automatic enemy detection** is already set to **$settingOption** for the world **$worldFormal**.",
      nowSetMessage = s":gear: **Automatic enemy detection** is now set to **$settingOption** for the world **$worldFormal**.",
      notConfiguredMessage = s"${Config.noEmoji} You need to run `/setup` and add **$worldFormal** before you can configure this setting.",
      adminLogMessage = s"<@$commandUser> set **automatic enemy detection** to **$settingOption** for the world **$worldFormal**.",
      adminLogThumbnail = "https://www.tibiawiki.com.br/wiki/Special:Redirect/file/Armillary_Sphere_(TibiaMaps).gif"
    )
  }

  private def detectHuntedsToDatabase(guild: Guild, world: String, detectSetting: String): Unit =
    worldConfigRepository.updateWorldString(guild.getId, domain.WorldName.formal(world), "detect_hunteds", detectSetting)

  def deathsLevelsHideShow(event: SlashCommandInteractionEvent, world: String, setting: String, playerType: String, channelType: String): MessageEmbed = {
    val worldFormal = domain.WorldName.formal(world)
    val guild = event.getGuild
    val commandUser = event.getUser.getId
    val settingType = if (setting == "show") "true" else "false"
    val thumbnailIcon = playerType match {
      case "allies"   => "Angel_Statue"
      case "neutrals" => "Guardian_Statue"
      case "enemies"  => "Stone_Coffin"
      case _          => ""
    }
    updateWorldSetting[String](
      guild, world, settingType,
      currentValue = w => playerType match {
        case "allies" =>
          if (channelType == "deaths") Some(w.showAlliesDeaths)
          else if (channelType == "levels") Some(w.showAlliesLevels)
          else None
        case "neutrals" =>
          if (channelType == "deaths") Some(w.showNeutralDeaths)
          else if (channelType == "levels") Some(w.showNeutralLevels)
          else None
        case "enemies" =>
          if (channelType == "deaths") Some(w.showEnemiesDeaths)
          else if (channelType == "levels") Some(w.showEnemiesLevels)
          else None
        case _ => None
      },
      applyValue = (w, v) => playerType match {
        case "allies" =>
          if (channelType == "deaths") w.copy(showAlliesDeaths = v)
          else if (channelType == "levels") w.copy(showAlliesLevels = v)
          else w
        case "neutrals" =>
          if (channelType == "deaths") w.copy(showNeutralDeaths = v)
          else if (channelType == "levels") w.copy(showNeutralLevels = v)
          else w
        case "enemies" =>
          if (channelType == "deaths") w.copy(showEnemiesDeaths = v)
          else if (channelType == "levels") w.copy(showEnemiesLevels = v)
          else w
        case _ => w
      },
      persist = v => deathsLevelsHideShowToDatabase(guild, world, v, playerType, channelType),
      alreadySetMessage = s"${Config.noEmoji} The **$channelType** channel is already set to **$setting $playerType** for the world **$worldFormal**.",
      nowSetMessage = s":gear: The **$channelType** channel is now set to **$setting $playerType** for the world **$worldFormal**.",
      notConfiguredMessage = s"${Config.noEmoji} You need to run `/setup` and add **$worldFormal** before you can configure this setting.",
      adminLogMessage = s"<@$commandUser> set the **$channelType** channel to **$setting $playerType** for the world **$worldFormal**.",
      adminLogThumbnail = s"https://www.tibiawiki.com.br/wiki/Special:Redirect/file/$thumbnailIcon.gif"
    )
  }

  def exivaList(event: SlashCommandInteractionEvent): MessageEmbed = {
    val options: Map[String, String] = event.getInteraction.getOptions.asScala.map(option => option.getName.toLowerCase() -> option.getAsString.trim()).toMap
    val worldOption: String = options.getOrElse("world", "")
    val settingOption: String = options.getOrElse("option", "")
    val settingType = if (settingOption == "show") "true" else "false"
    val worldFormal = domain.WorldName.formal(worldOption).trim
    val guild = event.getGuild
    val commandUser = event.getUser.getId
    updateWorldSetting[String](
      guild, worldOption, settingType,
      currentValue = w => Some(w.exivaList),
      applyValue = (w, v) => w.copy(exivaList = v),
      persist = v => exivaListToDatabase(guild, worldFormal, v),
      alreadySetMessage = s"${Config.noEmoji} The **exiva list on deaths** is already set to **$settingOption** for the world **$worldFormal**.",
      nowSetMessage = s":gear: **exiva list on deaths** is now set to **$settingOption** for the world **$worldFormal**.",
      notConfiguredMessage = s"${Config.noEmoji} You need to run `/setup` and add **$worldFormal** before you can configure this setting.",
      adminLogMessage = s"<@$commandUser> set **exiva list on deaths** to **$settingOption** for the world **$worldFormal**.",
      adminLogThumbnail = "https://www.tibiawiki.com.br/wiki/Special:Redirect/file/Find_Person.gif"
    )
  }

  private def exivaListToDatabase(guild: Guild, world: String, detectSetting: String): Unit =
    worldConfigRepository.updateWorldString(guild.getId, domain.WorldName.formal(world), "exiva_list", detectSetting)

  def onlineListConfig(event: SlashCommandInteractionEvent, world: String, setting: String): MessageEmbed = {
    val worldFormal = domain.WorldName.formal(world)
    val guild = event.getGuild
    val commandUser = event.getUser.getId
    val settingType = if (setting == "combine") "true" else "false"
    val embedBuild = new EmbedBuilder()
    embedBuild.setColor(BrandColor)
    val thumbnailIcon = "Blackboard"
    val cache = worldsData.getOrElse(guild.getId, List()).filter(w => w.name.toLowerCase() == world.toLowerCase())
    val existingSetting = cache.headOption.map(_.onlineCombined)
    if (existingSetting.isDefined) {
      if (existingSetting.get == settingType) {
        // embed reply
        embedBuild.setDescription(s"${Config.noEmoji} The online list is already set to **$setting** for the world **$worldFormal**.")
        embedBuild.build()
      } else {

        var disclaimer = ""

        val cache: Option[List[Worlds]] = worldsData.get(guild.getId) match {
          case Some(worlds) =>
            val filteredWorlds = worlds.filter(w => w.name.toLowerCase() == world.toLowerCase())
            if (filteredWorlds.nonEmpty) Some(filteredWorlds)
            else None
          case None => None
        }

        val categoryInfo: Option[String] = cache.flatMap(_.headOption.map(_.category))
        val alliesChannelInfo: Option[String] = cache.flatMap(_.headOption.map(_.alliesChannel))
        val enemiesChannelInfo: Option[String] = cache.flatMap(_.headOption.map(_.enemiesChannel))
        val neutralsChannelInfo: Option[String] = cache.flatMap(_.headOption.map(_.neutralsChannel))

        var category = guild.getCategoryById(categoryInfo.getOrElse("0"))
        val alliesChannel = guild.getTextChannelById(alliesChannelInfo.getOrElse("0"))
        val enemiesChannel = guild.getTextChannelById(enemiesChannelInfo.getOrElse("0"))
        val neutralsChannel = guild.getTextChannelById(neutralsChannelInfo.getOrElse("0"))

        val botRole = guild.getBotRole
        val publicRole = guild.getPublicRole

        if (setting == "combine") {

          if (event.getChannel.getId == alliesChannelInfo.getOrElse("0") || event.getChannel.getId == enemiesChannelInfo.getOrElse("0") || event.getChannel.getId == neutralsChannelInfo.getOrElse("0")) {
            embedBuild.setDescription(s"${Config.noEmoji} That command would delete this channel, run it somewhere else.")
            return embedBuild.build()
          }

          if (alliesChannel != null) {
            try {
              alliesChannel.delete().queue()
              disclaimer += s"\n- *The now unused `allies` channel has been deleted.*"
            } catch {
              case ex: Throwable => logger.warn(s"Failed to delete Channel ID: '${alliesChannelInfo}' for Guild ID: '${guild.getId}' Guild Name: '${guild.getName}' while combining the online list", ex)
            }
          }

          if (enemiesChannel != null) {
            try {
              enemiesChannel.delete().queue()
              disclaimer += s"\n- *The now unused `enemies` channel has been deleted.*"
            } catch {
              case ex: Throwable => logger.warn(s"Failed to delete Channel ID: '${enemiesChannelInfo}' for Guild ID: '${guild.getId}' Guild Name: '${guild.getName}' while combining the online list", ex)
            }
          }

          if (neutralsChannel != null) {
            try {
              neutralsChannel.delete().queue()
              disclaimer += s"\n- *The now unused `neutrals` channel has been deleted.*"
            } catch {
              case ex: Throwable => logger.warn(s"Failed to delete Channel ID: '${neutralsChannelInfo}' for Guild ID: '${guild.getId}' Guild Name: '${guild.getName}' while combining the online list", ex)
            }
          }

          // Now that separate channels are deleted, create a new 'online' channel
          try {
            if (category == null) {
              // create the category
              val newCategory = guild.createCategory(worldFormal).complete()
              grantWorldPerms(newCategory, botRole, publicRole)
              category = newCategory
              worldRepairConfig(guild, worldFormal, "category", newCategory.getId)

              // update the record in worldsData
              if (worldsData.contains(guild.getId)) {
                val worldsList = worldsData(guild.getId)
                val updatedWorldsList = worldsList.map { world =>
                  if (world.name.toLowerCase == worldFormal.toLowerCase) {
                    world.copy(category = newCategory.getId)
                  } else {
                    world
                  }
                }
                modifyWorldsData(_ + (guild.getId -> updatedWorldsList))
              }
            }
            // create the online channel
            val recreateAlliesChannel = guild.createTextChannel("📈・ᴏɴʟɪɴᴇ", category).complete()
            worldRepairConfig(guild, worldFormal, "allies_channel", recreateAlliesChannel.getId)
            // update the record in worldsData
            if (worldsData.contains(guild.getId)) {
              val worldsList = worldsData(guild.getId)
              val updatedWorldsList = worldsList.map { world =>
                if (world.name.toLowerCase == worldFormal.toLowerCase) {
                  world.copy(alliesChannel = recreateAlliesChannel.getId)
                } else {
                  world
                }
              }
              modifyWorldsData(_ + (guild.getId -> updatedWorldsList))
            }
            // apply permissions to created channel
            grantWorldPerms(recreateAlliesChannel, botRole, publicRole)
            disclaimer += s"\n- *You may want to move the new <#${recreateAlliesChannel.getId}> channel.*"
          } catch {
            case ex: Throwable => logger.warn(s"Failed to create category or online channels for Guild ID: '${guild.getId}' Guild Name: '${guild.getName}' while combining the online list", ex)
          }

        } else {
          // setting == "separate"

          if (event.getChannel.getId == alliesChannelInfo.getOrElse("0")) {
            embedBuild.setDescription(s"${Config.noEmoji} That command would delete this channel, run it somewhere else.")
            return embedBuild.build()
          }

          // get the bots main roles
          try {
            if (category == null) {
              // create the category
              val newCategory = guild.createCategory(worldFormal).complete()
              grantWorldPerms(newCategory, botRole, publicRole)
              category = newCategory
              worldRepairConfig(guild, worldFormal, "category", newCategory.getId)

              // update the record in worldsData
              if (worldsData.contains(guild.getId)) {
                val worldsList = worldsData(guild.getId)
                val updatedWorldsList = worldsList.map { world =>
                  if (world.name.toLowerCase == worldFormal.toLowerCase) {
                    world.copy(category = newCategory.getId)
                  } else {
                    world
                  }
                }
                modifyWorldsData(_ + (guild.getId -> updatedWorldsList))
              }
            } else {
              try {
                val categoryName = category.getName
                if (categoryName != s"${worldFormal}") {
                  val channelManager = category.getManager
                  channelManager.setName(s"${worldFormal}").queue()
                }
              } catch {
                case ex: Throwable => logger.warn(s"Failed to rename category for Guild ID: '${guild.getId}' Guild Name: '${guild.getName}' while separating the online list", ex)
              }
            }
            val channelList = ListBuffer[(TextChannel, Boolean)]()

            // delete the combined 'online' channel
            if (alliesChannel != null) {
              try {
                alliesChannel.delete().queue()
                disclaimer += s"\n- *The now unused `online` channel has been deleted.*"
              } catch {
                case ex: Throwable => logger.warn(s"Failed to delete Channel ID: '${alliesChannelInfo}' for Guild ID: '${guild.getId}' Guild Name: '${guild.getName}' while separating the online list", ex)
              }
            }

            // create the channels underneath the new/existing category
            val recreateAlliesChannel = guild.createTextChannel("🤍・ᴀʟʟɪᴇs", category).complete()
            channelList += ((recreateAlliesChannel, false))
            worldRepairConfig(guild, worldFormal, "allies_channel", recreateAlliesChannel.getId)
            // update the record in worldsData
            if (worldsData.contains(guild.getId)) {
              val worldsList = worldsData(guild.getId)
              val updatedWorldsList = worldsList.map { world =>
                if (world.name.toLowerCase == worldFormal.toLowerCase) {
                  world.copy(alliesChannel = recreateAlliesChannel.getId)
                } else {
                  world
                }
              }
              modifyWorldsData(_ + (guild.getId -> updatedWorldsList))
            }
            disclaimer += s"\n- *The channel <#${recreateAlliesChannel.getId}> has been recreated (you may want to move it).*"

            if (enemiesChannel == null) {
              val recreateEnemiesChannel = guild.createTextChannel("☠️・ᴇɴᴇᴍɪᴇs", category).complete()
              channelList += ((recreateEnemiesChannel, false))
              worldRepairConfig(guild, worldFormal, "enemies_channel", recreateEnemiesChannel.getId)
              // update the record in worldsData
              if (worldsData.contains(guild.getId)) {
                val worldsList = worldsData(guild.getId)
                val updatedWorldsList = worldsList.map { world =>
                  if (world.name.toLowerCase == worldFormal.toLowerCase) {
                    world.copy(enemiesChannel = recreateEnemiesChannel.getId)
                  } else {
                    world
                  }
                }
                modifyWorldsData(_ + (guild.getId -> updatedWorldsList))
              }
              disclaimer += s"\n- *The channel <#${recreateEnemiesChannel.getId}> has been recreated (you may want to move it).*"
            }

            if (neutralsChannel == null) {
              val recreateNeutralsChannel = guild.createTextChannel("📈・ɴᴇᴜᴛʀᴀʟs", category).complete()
              channelList += ((recreateNeutralsChannel, false))
              worldRepairConfig(guild, worldFormal, "neutrals_channel", recreateNeutralsChannel.getId)
              // update the record in worldsData
              if (worldsData.contains(guild.getId)) {
                val worldsList = worldsData(guild.getId)
                val updatedWorldsList = worldsList.map { world =>
                  if (world.name.toLowerCase == worldFormal.toLowerCase) {
                    world.copy(neutralsChannel = recreateNeutralsChannel.getId)
                  } else {
                    world
                  }
                }
                modifyWorldsData(_ + (guild.getId -> updatedWorldsList))
              }
              disclaimer += s"\n- *The channel <#${recreateNeutralsChannel.getId}> has been recreated (you may want to move it).*"
            }
            // apply required permissions to the new channel(s)
            if (channelList.nonEmpty) {
              channelList.foreach { case (channel, _) =>
                grantWorldPerms(channel, botRole, publicRole)
              }
            }
          } catch {
            case ex: Throwable => logger.warn(s"Failed to create category, allies, enemies or neutrals channels for Guild ID: '${guild.getId}' Guild Name: '${guild.getName}' while separating the online list", ex)
          }
        }

        // set the setting here
        val modifiedWorlds = worldsData(guild.getId).map { w =>
          if (w.name.toLowerCase() == world.toLowerCase()) {
            w.copy(onlineCombined = settingType)
          } else {
            w
          }
        }

        modifyWorldsData(_ + (guild.getId -> modifiedWorlds))
        onlineListConfigToDatabase(guild, world, settingType)

        val discordConfig = discordRetrieveConfig(guild)
        val adminChannelId = if (discordConfig.nonEmpty) discordConfig("admin_channel") else ""
        val adminChannel: TextChannel = guild.getTextChannelById(adminChannelId)
        presentation.AdminLog.post(adminChannel, s"<@$commandUser> set the online list channel to **$setting** for the world **$worldFormal**.\n$disclaimer", s"https://www.tibiawiki.com.br/wiki/Special:Redirect/file/$thumbnailIcon.gif")

        embedBuild.setDescription(s":gear: The online list channel is now set to **$setting** for the world **$worldFormal**.\n$disclaimer")
        embedBuild.build()
      }
    } else {
      embedBuild.setDescription(s"${Config.noEmoji} You need to run `/setup` and add **$worldFormal** before you can configure this setting.")
      embedBuild.build()
    }
  }

  private def onlineListConfigToDatabase(guild: Guild, world: String, setting: String): Unit =
    worldConfigRepository.updateWorldString(guild.getId, domain.WorldName.formal(world), "online_combined", setting)

  private def customSortConfig(guild: Guild, query: String): List[CustomSort] =
    customSortRepository.getAll(guild.getId)

  private def deathsLevelsHideShowToDatabase(guild: Guild, world: String, setting: String, playerType: String, channelType: String): Unit = {
    val worldFormal = domain.WorldName.formal(world)
    val tablePrefix = playerType match {
      case "allies" => "show_allies_"
      case "neutrals" => "show_neutral_"
      case "enemies" => "show_enemies_"
      case _ => ""
    }
    val tableName = s"$tablePrefix$channelType"
    worldConfigRepository.updateWorldString(guild.getId, worldFormal, tableName, setting)
  }

  def fullblessLevel(event: SlashCommandInteractionEvent, world: String, level: Int): MessageEmbed = {
    val worldFormal = domain.WorldName.formal(world)
    val guild = event.getGuild
    val commandUser = event.getUser.getId
    val embedBuild = new EmbedBuilder()
    embedBuild.setColor(BrandColor)
    val cache = worldsData.getOrElse(guild.getId, List()).filter(w => w.name.toLowerCase() == world.toLowerCase())
    val levelSetting = cache.headOption.map(_.fullblessLevel).getOrElse(null)
    if (levelSetting != null) {
      if (levelSetting == level) {
        // embed reply
        embedBuild.setDescription(s"${Config.noEmoji} The level to poke for **enemy fullblesses**\nis already set to **$level** for the world **$worldFormal**.")
        embedBuild.build()
      } else {
        // set the setting here
        val modifiedWorlds = worldsData(guild.getId).map { w =>
          if (w.name.toLowerCase() == world.toLowerCase()) {
            w.copy(fullblessLevel = level)
          } else {
            w
          }
        }
        modifyWorldsData(_ + (guild.getId -> modifiedWorlds))
        fullblessLevelToDatabase(guild, worldFormal, level)

        // edit the fullblesschannel embeds
        val worldConfigData = worldRetrieveConfig(guild, world)
        val discordConfig = discordRetrieveConfig(guild)
        val adminChannel = guild.getTextChannelById(discordConfig("admin_channel"))
        if (worldConfigData.nonEmpty) {
          val fullblessChannelId = worldConfigData("fullbless_channel")
          val channel: TextChannel = guild.getTextChannelById(fullblessChannelId)
          if (channel != null) {
            val messages = channel.getHistory.retrievePast(100).complete().asScala.filter(m => m.getAuthor.getId.equals(botUser))
            if (messages.nonEmpty) {
              val message = messages.head
              val fullblessRole = worldConfigData("fullbless_role")
              val nemesisRole = worldConfigData("nemesis_role")
              val allyPkRole = worldConfigData("allypk_role")
              val masslogRole = worldConfigData("masslog_role")

              // Fullbless Role
              message.editMessageEmbeds(fullblessRoleEmbed(worldFormal, fullblessRole, nemesisRole, allyPkRole, masslogRole, level))
                .setActionRow(fullblessRoleButtons: _*)
                .queue()
            }
          }
        }
        presentation.AdminLog.post(adminChannel, s"<@$commandUser> changed the level to poke for **enemy fullblesses**\nto **$level** for the world **$worldFormal**.", "https://www.tibiawiki.com.br/wiki/Special:Redirect/file/Amulet_of_Loss.gif")

        embedBuild.setDescription(s":gear: The level to poke for **enemy fullblesses**\nis now set to **$level** for the world **$worldFormal**.")
        embedBuild.build()
      }
    } else {
      embedBuild.setDescription(s"${Config.noEmoji} You need to run `/setup` and add **$worldFormal** before you can configure this setting.")
      embedBuild.build()
    }
  }

  def leaderboards(event: SlashCommandInteractionEvent, world: String, callback: MessageEmbed => Unit): Unit = {
    val worldFormal = domain.WorldName.formal(world)
    val embedBuild = new EmbedBuilder()
    embedBuild.setColor(BrandColor)

    if (Config.worldList.exists(_.equalsIgnoreCase(world))) {
      // Get the high scores
      val highScores: Future[Either[String, HighscoresResponse]] = tibiaDataClient.getHighscores(worldFormal, 1)

      // Handle the Future result asynchronously
      highScores.onComplete {
        case scala.util.Success(Right(highscoreResponse)) =>
          val currentPage = highscoreResponse.highscores.highscore_page.current_page
          val totalPages = highscoreResponse.highscores.highscore_page.total_pages
          embedBuild.setDescription(s"Current page: $currentPage\nTotal pages: $totalPages.")
          callback(embedBuild.build())

        case scala.util.Success(Left(errorMessage)) =>
          embedBuild.setDescription(s"${Config.noEmoji} Failed to fetch highscores: $errorMessage")
          callback(embedBuild.build())

        case scala.util.Failure(exception) =>
          embedBuild.setDescription(s"${Config.noEmoji} An error occurred: ${exception.toString}")
          callback(embedBuild.build())
      }
    } else {
      embedBuild.setDescription(s"${Config.noEmoji} **$worldFormal** is not a valid world.")
      callback(embedBuild.build())
    }
  }


  private def worldRepairConfig(guild: Guild, world: String, tableName: String, newValue: String): Unit =
    worldConfigRepository.updateWorldString(guild.getId, world, tableName, newValue)

  def minLevel(event: SlashCommandInteractionEvent, world: String, level: Int, levelsOrDeaths: String): MessageEmbed = {
    val worldFormal = domain.WorldName.formal(world)
    val guild = event.getGuild
    val commandUser = event.getUser.getId
    updateWorldSetting[Int](
      guild, world, level,
      currentValue = w => Some(if (levelsOrDeaths == "levels") w.levelsMin else w.deathsMin),
      applyValue = (w, v) => if (levelsOrDeaths == "levels") w.copy(levelsMin = v) else w.copy(deathsMin = v),
      persist = v => minLevelToDatabase(guild, worldFormal, v, levelsOrDeaths),
      alreadySetMessage = s"${Config.noEmoji} The minimum level for the **$levelsOrDeaths channel**\nis already set to `$level` for the world **$worldFormal**.",
      nowSetMessage = s":gear: The minimum level for the **$levelsOrDeaths channel**\nis now set to `$level` for the world **$worldFormal**.",
      notConfiguredMessage = s"${Config.noEmoji} You need to run `/setup` and add **$worldFormal** before you can configure this setting.",
      adminLogMessage = s"<@$commandUser> changed the minimum level for the **$levelsOrDeaths channel**\nto `$level` for the world **$worldFormal**.",
      adminLogThumbnail = "https://www.tibiawiki.com.br/wiki/Special:Redirect/file/Royal_Fanfare.gif"
    )
  }

  private def fullblessLevelToDatabase(guild: Guild, world: String, level: Int): Unit =
    worldConfigRepository.updateWorldInt(guild.getId, world, "fullbless_level", level)

  private def minLevelToDatabase(guild: Guild, world: String, level: Int, levelOrDeath: String): Unit = {
    val columnName = if (levelOrDeath == "levels") "levels_min" else "deaths_min"
    worldConfigRepository.updateWorldInt(guild.getId, world, columnName, level)
  }

  /** Build the boosted boss + creature + server-save embeds and post them to a
   *  guild's notifications channel with the server-save button, storing the
   *  message id so the daily scheduler can edit it later. Used when /setup or
   *  /repair (re)creates the notifications channel. */
  private def postBoostedNotifications(channel: TextChannel, guild: Guild, world: String): Unit = {
    val combinedFutures: Future[List[MessageEmbed]] = for {
      bossEmbed <- boostedService.boostedBossEmbed()
      creatureEmbed <- boostedService.boostedCreatureEmbed()
    } yield List(bossEmbed, creatureEmbed)

    combinedFutures.map { embeds =>
      val allEmbeds = embeds ++ serverSaveExtraEmbeds(world)
      channel
        .sendMessageEmbeds(allEmbeds.asJava)
        .setActionRow(Button.primary("boosted list", "Server Save Notifications").withEmoji(Emoji.fromFormatted(Config.letterEmoji)))
        .queue(
          (message: Message) => discordUpdateConfig(guild, "", "", "", message.getId, world),
          (e: Throwable) => logger.warn(s"Failed to send boosted boss/creature message for Guild ID: '${guild.getId}' Guild Name: '${guild.getName}':", e)
        )
    }
  }

  /** Post a channel's intro/help embed (the "this channel shows ..." message)
   *  if the channel exists. Used for the levels/deaths/activity channels. */
  private def postChannelIntro(channel: TextChannel, description: String): Unit =
    if (channel != null) {
      val embed = new EmbedBuilder()
      embed.setDescription(description)
      embed.setThumbnail("https://www.tibiawiki.com.br/wiki/Special:Redirect/file/Sign_(Library).gif")
      embed.setColor(BrandColor)
      channel.sendMessageEmbeds(embed.build()).queue()
    }

  /** Post the Galthen's Satchel cooldown-tracker embed + button into a guild's
   *  notifications channel (done on every /setup and /repair of that channel). */
  private def postGalthenTracker(channel: TextChannel): Unit = {
    val galthenEmbed = new EmbedBuilder()
    galthenEmbed.setColor(BrandColor)
    galthenEmbed.setDescription("This is a **[Galthen's Satchel](https://www.tibiawiki.com.br/wiki/Galthen's_Satchel)** cooldown tracker.\nManage your cooldowns here:")
    galthenEmbed.setThumbnail("https://www.tibiawiki.com.br/wiki/Special:Redirect/file/Galthen's_Satchel.gif")
    channel.sendMessageEmbeds(galthenEmbed.build()).addActionRow(
      Button.primary("galthen default", "Cooldowns").withEmoji(Emoji.fromFormatted(Config.satchelEmoji))
    ).queue()
  }

  /** Apply the standard per-world channel/category permissions: grant the bot
   *  the channel-management set and deny @everyone the ability to post. Used for
   *  the world category and each world channel in /setup and /repair. */
  private def grantWorldPerms(entity: IPermissionContainer, botRole: Role, publicRole: Role): Unit = {
    entity.upsertPermissionOverride(botRole)
      .grant(Permission.VIEW_CHANNEL)
      .grant(Permission.MESSAGE_SEND)
      .grant(Permission.MESSAGE_MENTION_EVERYONE)
      .grant(Permission.MESSAGE_EMBED_LINKS)
      .grant(Permission.MESSAGE_HISTORY)
      .grant(Permission.MANAGE_CHANNEL)
      .complete()
    entity.upsertPermissionOverride(publicRole).deny(Permission.MESSAGE_SEND).complete()
  }

  /** Delete a world's role if it still exists, logging (not throwing) on failure. */
  private def creatureImageUrl(creature: String): String =
    presentation.Urls.creatureImageUrl(creature, Config.creatureUrlMappings)

  def creatureWikiUrl(creature: String): String =
    presentation.Urls.creatureWikiUrl(creature, Config.creatureUrlMappings)

  // Death screenshot database methods
  def storeDeathScreenshot(guildId: String, world: String, characterName: String, deathTime: Long, screenshotUrl: String, addedBy: String, addedName: String, messageId: String): Unit =
    deathScreenshotRepository.store(guildId, world, characterName, deathTime, screenshotUrl, addedBy, addedName, messageId)

  def getDeathScreenshots(guildId: String, world: String, characterName: String, deathTime: Long): List[DeathScreenshot] =
    deathScreenshotRepository.get(guildId, world, characterName, deathTime)

  def deleteDeathScreenshot(guildId: String, world: String, characterName: String, deathTime: Long, screenshotUrl: String, userId: String): Boolean = {
    val guild = discordGateway.guildById(guildId)
    val member = guild.retrieveMemberById(userId).complete()
    val admin = member != null && (member.hasPermission(Permission.MANAGE_SERVER) || member.hasPermission(Permission.MESSAGE_MANAGE))
    deathScreenshotRepository.deleteIfPermitted(guildId, characterName, deathTime, screenshotUrl) { addedBy =>
      addedBy == userId || admin
    }
  }

  def fetchDreamScarBosses(): List[BossEntry] = wikiClient.dreamScarBosses()

  def fetchCreatureNames(): List[String] = wikiClient.creatureNames()

  def advanceDromeTime(inputTime: Instant): Unit =
    dromeTime = domain.time.DromeCycle.advanceFrom(dromeTime, inputTime)

  def shiftAllBossesUp(current: Map[String, String]): Map[String, String] =
    domain.time.DreamScarCycle.shiftAllBossesUp(current)

}
