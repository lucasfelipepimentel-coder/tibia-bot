package com.tibiabot.commands.handlers

import com.tibiabot.BotApp
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent

/** Handles the channel-management commands: `/setup`, `/remove`, `/repair`. */
object ChannelCommands {

  def setup(event: SlashCommandInteractionEvent): Unit = {
    val embed = BotApp.channelService.createChannels(event)
    event.getHook.sendMessageEmbeds(embed).queue()
  }

  def remove(event: SlashCommandInteractionEvent): Unit = {
    val embed = BotApp.channelService.removeChannels(event)
    event.getHook.sendMessageEmbeds(embed).queue()
  }

  def repair(event: SlashCommandInteractionEvent): Unit = {
    val worldOption = Options.of(event).getOrElse("world", "")
    val embed = BotApp.channelService.repairChannel(event, worldOption)
    event.getHook.sendMessageEmbeds(embed).queue()
  }
}
