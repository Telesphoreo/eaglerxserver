# Paper 26.2 compatibility

This branch adds a compatibility path for running the Bukkit build of EaglerXServer directly on Paper 26.2 while preserving the legacy Spigot/Paper code paths.

## Recommended topology

EaglerXServer is still best deployed on Velocity or BungeeCord in front of Paper. The direct Paper plugin performs version-sensitive network and login injection, so every major Paper networking change can require another compatibility update.

For a direct Paper 26.2 installation:

1. Run Paper 26.2 with Java 25.
2. Build `EaglerXServer.jar` with `gradlew core:shadowJar` (`gradlew.bat` on Windows).
3. Copy `core/build/libs/EaglerXServer.jar` into the Paper server's `plugins` directory.
4. Install ViaVersion, ViaBackwards, and ViaRewind so the modern backend can accept the legacy Minecraft protocols used by Eaglercraft clients.
5. Start Paper once and review `plugins/EaglercraftXServer/listener.yml` and `settings.yml`.
6. Put a TLS-capable reverse proxy such as Caddy or nginx in front of the listener for public `wss://` access, or configure the plugin's listener TLS settings.

The Bukkit plugin declares `api-version: 1.13`, which avoids Paper's legacy-plugin warning and legacy material initialization while retaining compatibility with modern Bukkit servers. It no longer loads on Bukkit versions older than 1.13.

## Build and verification

```powershell
.\gradlew.bat clean build core:shadowJar
```

The project is compiled for Java 25. The Paper 26.2 compatibility fixes are reflection-based, so the project does not need to replace its Paper 1.12.2 compile-only dependency.

The compatibility smoke tests used Paper 26.2 builds 87 and 111 with Java 25 on Windows and Linux. They verified:

- plugin load, enable, listener registration, and clean disable;
- native Netty Epoll initialization on Linux without relying on fork-specific server-property accessors;
- an HTTP WebSocket upgrade on the Minecraft port (`101 Switching Protocols`);
- an Eagler handshake protocol V5 login through allow-login and finish-login;
- a Minecraft protocol 776 status response;
- a protocol 776 offline login through the compression and login-finished packets.

A real Eaglercraft browser client and the optional companion plugins should still be tested before production deployment.

## Module map

- `protocol-game`: EaglercraftX 1.8 game/plugin-message packet definitions and codecs.
- `api`: cross-platform public API; `api-bukkit`, `api-bungee`, and `api-velocity` add platform event types.
- `core-config`: shared YAML/TOML configuration model and serializers.
- `core`: WebSocket detection, handshake/login state machine, Netty pipeline transformation, skins, voice, webview, commands, update checks, and the common server runtime.
- `core/core-platform-*`: Bukkit, BungeeCord, and Velocity adapters. The Paper 26.2 compatibility work is primarily in `core-platform-bukkit`.
- `skin-cache`: shared skin lookup/cache support; `sqlite-jdbc-jar` supplies the bundled SQLite driver.
- `rewind_v1_5`: optional Eaglercraft 1.5 packet translator plus platform adapters.
- `eaglermotd`: optional MOTD/query extension plus platform adapters.
- `eaglerweb`: optional HTTP static-file hosting plus platform adapters.
- `backend-rpc-api`, `backend-rpc-protocol`, and `backend-rpc-core`: proxy-to-backend API bridge; the backend plugin is Bukkit-only.
- `voice-rpc-protocol`: packets used to relay voice state over the backend RPC link.
- `plan`: optional Plan analytics integration plus platform adapters.
- `supervisor-protocol` and `supervisor-core`: protocol and standalone daemon for coordinating multiple proxies.
- `stubs`: compile-time compatibility stubs used by platform-specific implementations.

## Paper 26.2 changes handled here

- `MinecraftServer#getServerConnection()` became `getConnection()`, and Netty groups moved to `EventLoopGroupHolder`.
- native-transport configuration lookup no longer aborts startup when a Paper fork removes its internal dedicated-server property accessors.
- modern login compression callbacks are suppressed after the Eagler pipeline replaces Paper's vanilla `splitter`, preventing `Connection#setupCompression` from mutating a handler that no longer exists.
- the connection direction type is now Mojang-mapped `PacketFlow`.
- `Connection#send` overloads and login/compression packet names changed.
- login listeners use Mojang-mapped names, a transferred-login constructor argument, and a different state enum name.
- `ServerPlayer`, `ServerGamePacketListenerImpl`, and `Connection` replaced the old Spigot-mapped entity/player/network-manager names.
- Authlib 9 uses record-style accessors and may expose immutable profile properties, so the Bukkit adapter now uses version-neutral accessors and creates a mutable replacement profile when Eagler properties must be injected.
