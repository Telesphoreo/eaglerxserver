package net.lax1dude.eaglercraft.backend.server.base.pipeline;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;

/** Runs protocol packets through the production guard and WebSocket frame codec. */
public class ItemNameIntegrationTest {
	private static int checks;
	private enum Packet { SLOT, INVENTORY, EQUIPMENT, METADATA, MOB, PLAYER, TRADE }

	public static void main(String[] args) throws Exception {
		String name;
		try (var in = ItemNameIntegrationTest.class.getResourceAsStream("/long-item-name.txt")) {
			name = new String(in.readAllBytes(), StandardCharsets.UTF_8);
		}
		check(name.length() == 18000 && name.chars().allMatch(c -> c == '#'),
				"Fixture must contain the exact name from hotbar.nbt row 4, slot 1");
		for (int protocol : new int[] { 47, 340, 498, 755, 756 }) {
			for (Packet kind : Packet.values()) {
				if (protocol >= 755 && (kind == Packet.MOB || kind == Packet.PLAYER)) continue;
				String wireName = protocol <= 340 ? name : "{\"text\":\"" + name + "\"}";
				assertPacket(protocol, packet(protocol, kind, wireName), packet(protocol, kind, null), true);
				for (String normal : new String[] { null, "", "Dinnerbone", "\u00a7aTools \ud83d\udd27", "#".repeat(4096) }) {
					assertPacket(protocol, packet(protocol, kind, normal), packet(protocol, kind, normal), true);
				}
			}
			// The byte budget must handle NBT's modified UTF-8, not split encoded text or JSON.
			assertPacket(protocol, packet(protocol, Packet.SLOT, "\u00a7".repeat(3000)),
					packet(protocol, Packet.SLOT, null), true);
			// Fail atomically if a packet ends midway through a later item.
			ByteBuf truncated = packet(protocol, Packet.INVENTORY, name);
			truncated.writerIndex(truncated.writerIndex() - 2);
			assertPacket(protocol, truncated, truncated.copy(), true);
			ByteBuf trailing = packet(protocol, Packet.SLOT, name).writeByte(99);
			assertPacket(protocol, trailing, trailing.copy(), true);
			// A separate Java connection does not install the guard.
			ByteBuf javaPacket = packet(protocol, Packet.SLOT, name);
			assertPacket(protocol, javaPacket, javaPacket.copy(), false);
		}
		ByteBuf unknown = packet(340, Packet.SLOT, name);
		assertPacket(999, unknown, unknown.copy(), true);
		ByteBuf unrelated = Unpooled.buffer().writeByte(0x7E).writeBytes(name.getBytes(StandardCharsets.UTF_8));
		assertPacket(340, unrelated, unrelated.copy(), true);
		System.out.println("PASS: " + checks + " assertions across five protocols; supplied name, ordinary names, "
				+ "inventory, cursor, equipment, metadata, spawns, trades, malformed packets and Java passthrough");
	}

	private static void assertPacket(int protocol, ByteBuf input, ByteBuf expected, boolean eagler) {
		// Use a read-only slice with a nonzero reader index. Keep another owner to model a
		// packet shared with a Java connection and detect accidental in-place changes.
		ByteBuf backing = Unpooled.directBuffer(input.readableBytes() + 3).writeZero(3).writeBytes(input);
		input.release();
		backing.readerIndex(3);
		ByteBuf original = backing.copy();
		ByteBuf sent = backing.asReadOnly().retain();
		EmbeddedChannel channel = new EmbeddedChannel(WebSocketEaglerFrameCodec.INSTANCE);
		if (eagler) {
			// Match the guard order before the handshake handler is removed.
			channel.pipeline().addLast(new EquipmentNBTGuard(protocol, () -> {}), new ItemNameGuard(protocol));
		}
		try {
			check(channel.writeOutbound(sent), "No WebSocket output");
			BinaryWebSocketFrame frame = channel.readOutbound();
			try {
				check(frame.content().equals(expected), "Incorrect packet for protocol " + protocol
						+ ": expected " + expected.readableBytes() + " bytes, got " + frame.content().readableBytes());
				check(backing.equals(original) && backing.readerIndex() == 3, "Shared source packet was changed");
				check(channel.readOutbound() == null, "Extra packet");
			} finally { frame.release(); }
		} finally {
			channel.finishAndReleaseAll();
			expected.release();
			original.release();
			check(backing.refCnt() == 1, "Packet reference leak");
			backing.release();
		}
	}

	private static ByteBuf packet(int p, Packet kind, String name) throws IOException {
		// Layouts checked against minecraft-data's protocol schemas for each version.
		int id = switch (kind) {
			case SLOT -> p == 47 ? 0x2F : 0x16;
			case INVENTORY -> p == 47 ? 0x30 : 0x14;
			case EQUIPMENT -> p == 47 ? 4 : p == 340 ? 0x3F : p == 498 ? 0x46 : 0x50;
			case METADATA -> p == 47 ? 0x1C : p == 340 ? 0x3C : p == 498 ? 0x43 : 0x4D;
			case MOB -> p == 47 ? 0x0F : 3;
			case PLAYER -> p == 47 ? 0x0C : 5;
			case TRADE -> p == 47 ? 0x3F : p == 340 ? 0x18 : p == 498 ? 0x27 : 0x28;
		};
		ByteBuf b = Unpooled.buffer();
		varInt(b, id);
		switch (kind) {
			case SLOT -> {
				b.writeByte(-1); // Include cursor updates as well as inventory slots.
				if (p == 756) varInt(b, 300);
				b.writeShort(-1);
				item(b, p, name);
			}
			case INVENTORY -> {
				b.writeByte(0);
				if (p == 756) { varInt(b, 300); varInt(b, 3); }
				else b.writeShort(3);
				item(b, p, "Normal item");
				emptyItem(b, p);
				item(b, p, name);
				if (p == 756) item(b, p, name);
			}
			case EQUIPMENT -> {
				varInt(b, 300);
				if (p == 47) b.writeShort(0);
				else b.writeByte(p >= 755 ? 0x80 : 0);
				item(b, p, name);
				if (p >= 755) {
					b.writeByte(0x81); emptyItem(b, p);
					b.writeByte(5); item(b, p, name);
				}
			}
			case METADATA, MOB, PLAYER -> {
				varInt(b, 300);
				if (kind == Packet.MOB) {
					if (p == 47) b.writeZero(22);
					else { b.writeZero(16); varInt(b, 300); b.writeZero(33); }
				} else if (kind == Packet.PLAYER) {
					b.writeZero(p == 47 ? 32 : 42);
				}
				metadata(b, p, name);
			}
			case TRADE -> {
				if (p <= 340) { string(b, "MC|TrList"); b.writeInt(3); }
				else varInt(b, 3);
				b.writeByte(2);
				for (int i = 0; i < 2; ++i) {
					item(b, p, "Emerald"); item(b, p, name);
					b.writeBoolean(i == 0);
					if (i == 0) item(b, p, name);
					b.writeBoolean(false).writeInt(1).writeInt(10);
					if (p >= 498) b.writeInt(2).writeInt(-1).writeFloat(0.2f).writeInt(3);
				}
				if (p >= 498) { varInt(b, 2); varInt(b, 300); b.writeBoolean(true).writeBoolean(false); }
			}
		}
		return b;
	}

	private static void metadata(ByteBuf b, int p, String name) throws IOException {
		if (p == 47) {
			b.writeByte(0).writeByte(0);
			b.writeByte((4 << 5) | 2); string(b, "Entity");
			b.writeByte((5 << 5) | 10); item(b, p, name);
			b.writeByte((2 << 5) | 11).writeInt(123).writeByte(127);
		} else {
			b.writeByte(0).writeByte(0).writeByte(0);
			b.writeByte(2).writeByte(p == 340 ? 3 : 5);
			if (p != 340) b.writeBoolean(true);
			string(b, "Entity");
			if (p >= 498) {
				b.writeByte(7).writeByte(15); varInt(b, p == 498 ? 14 : 15); b.writeZero(16);
			}
			b.writeByte(10).writeByte(p == 340 ? 5 : 6); item(b, p, name);
			b.writeByte(11).writeByte(1); varInt(b, 12345);
			b.writeByte(255);
		}
	}

	private static void item(ByteBuf b, int p, String name) throws IOException {
		if (p <= 340) b.writeShort(421).writeByte(3).writeShort(7);
		else { b.writeBoolean(true); varInt(b, 421); b.writeByte(3); }
		try (ByteBufOutputStream out = new ByteBufOutputStream(b)) {
			out.writeByte(10); out.writeUTF("");
			out.writeByte(1); out.writeUTF("Unbreakable"); out.writeByte(1);
			out.writeByte(10); out.writeUTF("display");
			if (name != null) { out.writeByte(8); out.writeUTF("Name"); out.writeUTF(name); }
			out.writeByte(3); out.writeUTF("color"); out.writeInt(0x123456);
			out.writeByte(9); out.writeUTF("Lore"); out.writeByte(8); out.writeInt(1); out.writeUTF("Keep this lore");
			out.writeByte(0);
			// Preserve plugin data named Name outside display, even when it is long.
			out.writeByte(8); out.writeUTF("Name"); out.writeUTF("x".repeat(5000));
			// Also cover a nested item in a container's BlockEntityTag.Items list.
			out.writeByte(10); out.writeUTF("BlockEntityTag");
			out.writeByte(9); out.writeUTF("Items"); out.writeByte(10); out.writeInt(1);
			out.writeByte(10); out.writeUTF("tag");
			out.writeByte(10); out.writeUTF("display");
			if (name != null) { out.writeByte(8); out.writeUTF("Name"); out.writeUTF(name); }
			out.writeByte(0); out.writeByte(0); out.writeByte(0); out.writeByte(0);
			out.writeByte(0);
		}
	}

	private static void emptyItem(ByteBuf b, int p) {
		if (p <= 340) b.writeShort(-1);
		else b.writeBoolean(false);
	}

	private static void string(ByteBuf b, String text) {
		byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
		varInt(b, bytes.length);
		b.writeBytes(bytes);
	}

	private static void varInt(ByteBuf b, int value) {
		while ((value & ~127) != 0) {
			b.writeByte((value & 127) | 128);
			value >>>= 7;
		}
		b.writeByte(value);
	}

	private static void check(boolean condition, String message) {
		++checks;
		if (!condition) throw new AssertionError(message);
	}
}
