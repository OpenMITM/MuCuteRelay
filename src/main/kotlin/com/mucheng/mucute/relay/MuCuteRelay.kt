package com.mucheng.mucute.relay

import com.mucheng.mucute.relay.MuCuteRelaySession.ClientSession
import com.mucheng.mucute.relay.address.MuCuteAddress
import com.mucheng.mucute.relay.address.inetSocketAddress
import io.netty.bootstrap.Bootstrap
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.Channel
import io.netty.channel.ChannelFuture
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioDatagramChannel
import org.cloudburstmc.netty.channel.raknet.RakChannelFactory
import org.cloudburstmc.netty.channel.raknet.config.RakChannelOption
import org.cloudburstmc.netty.handler.codec.raknet.server.RakServerRateLimiter
import org.cloudburstmc.protocol.bedrock.BedrockPeer
import org.cloudburstmc.protocol.bedrock.BedrockPong
import org.cloudburstmc.protocol.bedrock.PacketDirection
import org.cloudburstmc.protocol.bedrock.codec.BedrockCodec
import org.cloudburstmc.protocol.bedrock.codec.v766.Bedrock_v766
import org.cloudburstmc.protocol.bedrock.netty.initializer.BedrockChannelInitializer
import kotlin.random.Random

class MuCuteRelay(
    val localAddress: MuCuteAddress = MuCuteAddress("0.0.0.0", 19132),
    val advertisement: BedrockPong = DefaultAdvertisement
) {

    @Suppress("MemberVisibilityCanBePrivate")
    companion object {

        val DefaultCodec: BedrockCodec = Bedrock_v766.CODEC

        val DefaultAdvertisement: BedrockPong = BedrockPong()
            .edition("MCPE")
            .gameType("Survival")
            .version(DefaultCodec.minecraftVersion)
            .protocolVersion(DefaultCodec.protocolVersion)
            .motd("MuCuteRelay")
            .playerCount(0)
            .maximumPlayerCount(20)
            .subMotd("YouTube: MrPokeG")
            .nintendoLimited(false)

    }

    @Suppress("MemberVisibilityCanBePrivate")
    val isRunning: Boolean
        get() = channelFuture != null

    private var channelFuture: ChannelFuture? = null

    internal var muCuteRelaySession: MuCuteRelaySession? = null

    var remoteAddress: MuCuteAddress? = null
        internal set

    fun capture(
        remoteAddress: MuCuteAddress = MuCuteAddress("geo.hivebedrock.network", 19132),
        onSessionCreated: MuCuteRelaySession.() -> Unit
    ): MuCuteRelay {
        if (isRunning) {
            return this
        }

        this.remoteAddress = remoteAddress

        advertisement
            .ipv4Port(localAddress.port)
            .ipv6Port(localAddress.port)

        ServerBootstrap()
            .group(NioEventLoopGroup())
            .channelFactory(RakChannelFactory.server(NioDatagramChannel::class.java))
            .option(RakChannelOption.RAK_ADVERTISEMENT, advertisement.toByteBuf())
            .option(RakChannelOption.RAK_GUID, Random.nextLong())
            .childHandler(object : BedrockChannelInitializer<MuCuteRelaySession.ServerSession>() {

                override fun createSession0(peer: BedrockPeer, subClientId: Int): MuCuteRelaySession.ServerSession {
                    return MuCuteRelaySession(peer, subClientId, this@MuCuteRelay)
                        .also {
                            muCuteRelaySession = it
                            it.onSessionCreated()
                        }
                        .server
                }

                override fun initSession(session: MuCuteRelaySession.ServerSession) {}

                override fun preInitChannel(channel: Channel) {
                    channel.attr(PacketDirection.ATTRIBUTE).set(PacketDirection.CLIENT_BOUND)
                    super.preInitChannel(channel)
                }

            })
            .localAddress(localAddress.inetSocketAddress)
            .bind()
            .awaitUninterruptibly()
            .also {
                it.channel().pipeline().remove(RakServerRateLimiter.NAME)
                channelFuture = it
            }

        return this
    }

    internal fun connectToServer(onSessionCreated: ClientSession.() -> Unit) {
        val clientGUID = Random.nextLong()

        Bootstrap()
            .group(NioEventLoopGroup())
            .channelFactory(RakChannelFactory.client(NioDatagramChannel::class.java))
            .option(RakChannelOption.RAK_PROTOCOL_VERSION, 11)
            .option(RakChannelOption.RAK_GUID, clientGUID)
            .option(RakChannelOption.RAK_REMOTE_GUID, clientGUID)
            .option(RakChannelOption.RAK_CONNECT_TIMEOUT, Long.MAX_VALUE)
            .handler(object : BedrockChannelInitializer<ClientSession>() {

                override fun createSession0(peer: BedrockPeer, subClientId: Int): ClientSession {
                    return muCuteRelaySession!!.ClientSession(peer, subClientId)
                }

                override fun initSession(clientSession: ClientSession) {
                    muCuteRelaySession!!.client = clientSession
                    onSessionCreated(clientSession)
                }

                override fun preInitChannel(channel: Channel) {
                    channel.attr(PacketDirection.ATTRIBUTE).set(PacketDirection.SERVER_BOUND)
                    super.preInitChannel(channel)
                }

            })
            .remoteAddress(remoteAddress!!.inetSocketAddress)
            .connect()
            .awaitUninterruptibly()
    }

    fun disconnect() {
        if (!isRunning) {
            return
        }

        channelFuture?.channel()?.also {
            it.close().awaitUninterruptibly()
            it.parent().close().awaitUninterruptibly()
        }
        channelFuture = null
        muCuteRelaySession = null
    }

}