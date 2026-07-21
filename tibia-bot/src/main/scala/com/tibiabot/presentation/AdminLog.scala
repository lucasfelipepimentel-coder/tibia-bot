package com.tibiabot.presentation

import com.tibiabot.Config
import net.dv8tion.jda.api.EmbedBuilder
import net.dv8tion.jda.api.entities.channel.concrete.TextChannel

/** Posts a "command was run" audit embed to a guild's command-log channel, if
 *  it exists and is writable. Centralises the block repeated by every command
 *  that audits itself (title/colour are fixed; description/thumbnail vary).
 *  Extracted verbatim from BotApp.postAdminLog. */
object AdminLog {
  def post(adminChannel: TextChannel, description: String, thumbnail: String): Unit =
    if (adminChannel != null && (adminChannel.canTalk() || !Config.prod)) {
      val adminEmbed = new EmbedBuilder()
      adminEmbed.setTitle(":gear: a command was run:")
      adminEmbed.setDescription(description)
      adminEmbed.setThumbnail(thumbnail)
      adminEmbed.setColor(Embeds.BrandColor)
      adminChannel.sendMessageEmbeds(adminEmbed.build()).queue()
    }
}
