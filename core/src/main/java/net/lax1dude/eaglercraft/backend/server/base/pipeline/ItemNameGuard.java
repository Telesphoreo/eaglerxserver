package net.lax1dude.eaglercraft.backend.server.base.pipeline;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

/** Removes oversized display names from item packets sent to Eagler clients. */
public final class ItemNameGuard extends ChannelOutboundHandlerAdapter {

	// EaglerFontRenderer uses an unchecked int[6553] when decoding text. Reserve room
	// for client-added formatting. A byte limit also bounds UTF-16 length in NBT UTF.
	private static final int MAX_NAME_BYTES = 4096;
	private final int protocol;

	public ItemNameGuard(int protocol) {
		this.protocol = protocol;
	}

	public static boolean supports(int protocol) {
		return protocol == 47 || protocol == 340 || protocol == 498 || protocol == 755 || protocol == 756;
	}

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		if (msg instanceof ByteBuf original && supports(protocol)) {
			ByteBuf replacement = sanitize(original);
			if (replacement != original) {
				original.release();
				msg = replacement;
			}
		}
		ctx.write(msg, promise);
	}

	private ByteBuf sanitize(ByteBuf original) {
		Scanner scan = new Scanner(original.duplicate(), protocol);
		try {
			if (!scan.packet() || scan.in.isReadable() || scan.removals.isEmpty()) return original;
		} catch (IndexOutOfBoundsException | IllegalArgumentException ex) {
			// Leave malformed or unknown layouts to the normal protocol handler.
			return original;
		}
		int size = original.readableBytes();
		for (Range range : scan.removals) size -= range.end - range.start;
		ByteBuf out = original.alloc().buffer(size);
		try {
			int cursor = original.readerIndex();
			for (Range range : scan.removals) {
				out.writeBytes(original, cursor, range.start - cursor);
				cursor = range.end;
			}
			out.writeBytes(original, cursor, original.writerIndex() - cursor);
			return out;
		} catch (Throwable ex) {
			out.release();
			throw ex;
		}
	}

	private record Range(int start, int end) {}

	private static final class Scanner {
		final ByteBuf in;
		final int protocol;
		final List<Range> removals = new ArrayList<>();
		int budget;

		Scanner(ByteBuf in, int protocol) {
			this.in = in;
			this.protocol = protocol;
			this.budget = in.readableBytes();
		}

		boolean packet() {
			int id = varInt();
			if (id == (protocol == 47 ? 0x2F : 0x16)) {
				in.skipBytes(1);
				if (protocol == 756) varInt();
				in.skipBytes(2);
				item();
			} else if (id == (protocol == 47 ? 0x30 : 0x14)) {
				in.skipBytes(1);
				if (protocol == 756) varInt();
				int count = protocol == 756 ? varInt() : in.readShort();
				if (count < 0 || count > in.readableBytes()) throw new IllegalArgumentException("Item count");
				for (int i = 0; i < count; ++i) item();
				if (protocol == 756) item(); // Cursor item
			} else if (id == (protocol == 47 ? 0x04 : protocol == 340 ? 0x3F : protocol == 498 ? 0x46 : 0x50)) {
				varInt();
				int slot;
				do {
					slot = protocol == 47 ? in.readShort() : protocol >= 755 ? in.readUnsignedByte() : varInt();
					item();
				} while (protocol >= 755 && (slot & 0x80) != 0);
			} else if (id == (protocol == 47 ? 0x1C : protocol == 340 ? 0x3C : protocol == 498 ? 0x43 : 0x4D)) {
				varInt();
				metadata();
			} else if (protocol <= 498 && id == (protocol == 47 ? 0x0F : 0x03)) {
				varInt();
				if (protocol == 47) {
					in.skipBytes(22);
				} else {
					in.skipBytes(16);
					varInt();
					in.skipBytes(33);
				}
				metadata();
			} else if (protocol <= 498 && id == (protocol == 47 ? 0x0C : 0x05)) {
				varInt();
				in.skipBytes(protocol == 47 ? 32 : 42);
				metadata();
			} else if (protocol <= 340 && id == (protocol == 47 ? 0x3F : 0x18)) {
				if (!string().equals("MC|TrList")) return false;
				in.skipBytes(4);
				trades();
			} else if (protocol >= 498 && id == (protocol == 498 ? 0x27 : 0x28)) {
				varInt();
				trades();
				varInt();
				varInt();
				in.skipBytes(2);
			} else {
				return false;
			}
			return true;
		}

		void trades() {
			int count = in.readUnsignedByte();
			for (int i = 0; i < count; ++i) {
				item();
				item();
				if (in.readBoolean()) item();
				in.skipBytes(protocol <= 340 ? 9 : 25);
			}
		}

		void item() {
			if (protocol <= 340) {
				if (in.readShort() == -1) return;
				in.skipBytes(3);
			} else {
				if (!in.readBoolean()) return;
				varInt();
				in.skipBytes(1);
			}
			nbt(true);
		}

		void nbt(boolean item) {
			int type = in.readUnsignedByte();
			if (type == 0) return;
			if (type != 10) throw new IllegalArgumentException("NBT root");
			in.skipBytes(in.readUnsignedShort());
			value(type, 0, false, item);
		}

		void value(int type, int depth, boolean display, boolean item) {
			if (depth > 512 || --budget < 0) throw new IllegalArgumentException("NBT scan limit");
			switch (type) {
				case 1 -> in.skipBytes(1);
				case 2 -> in.skipBytes(2);
				case 3, 5 -> in.skipBytes(4);
				case 4, 6 -> in.skipBytes(8);
				case 7, 11, 12 -> {
					int count = in.readInt();
					int width = type == 7 ? 1 : type == 11 ? 4 : 8;
					if (count < 0 || count > in.readableBytes() / width) throw new IllegalArgumentException("NBT array length");
					in.skipBytes(count * width);
				}
				case 8 -> in.skipBytes(in.readUnsignedShort());
				case 9 -> {
					int child = in.readUnsignedByte();
					int count = in.readInt();
					if (count < 0 || count > budget || child > 12 || (child == 0 && count != 0)) {
						throw new IllegalArgumentException("NBT list");
					}
					for (int i = 0; i < count; ++i) value(child, depth + 1, false, item);
				}
				case 10 -> {
					int child;
					while ((child = in.readUnsignedByte()) != 0) {
						int start = in.readerIndex() - 1;
						int length = in.readUnsignedShort();
						boolean name = display && child == 8 && key(length, "Name");
						boolean childDisplay = item && child == 10 && key(length, "display");
						in.skipBytes(length);
						boolean remove = name && in.getUnsignedShort(in.readerIndex()) > MAX_NAME_BYTES;
						value(child, depth + 1, childDisplay, item);
						if (remove) removals.add(new Range(start, in.readerIndex()));
					}
				}
				default -> throw new IllegalArgumentException("NBT type");
			}
		}

		boolean key(int length, String name) {
			if (length != name.length()) return false;
			for (int i = 0; i < length; ++i) {
				if (in.getByte(in.readerIndex() + i) != name.charAt(i)) return false;
			}
			return true;
		}

		void metadata() {
			int header;
			while ((header = in.readUnsignedByte()) != (protocol == 47 ? 127 : 255)) {
				int type = protocol == 47 ? header >> 5 : varInt();
				if (protocol == 47) {
					switch (type) {
						case 0 -> in.skipBytes(1);
						case 1 -> in.skipBytes(2);
						case 2, 3 -> in.skipBytes(4);
						case 4 -> skipString();
						case 5 -> item();
						case 6, 7 -> in.skipBytes(12);
						default -> throw new IllegalArgumentException("Metadata type");
					}
				} else {
					// 1.13 inserted Optional Chat at index 5.
					if (protocol >= 498 && type == 5) {
						if (in.readBoolean()) skipString();
						continue;
					}
					if (protocol >= 498 && type > 5) --type;
					switch (type) {
						case 0, 6 -> in.skipBytes(1);
						case 1, 10, 12, 16, 17 -> varInt();
						case 2 -> in.skipBytes(4);
						case 3, 4 -> skipString();
						case 5 -> item();
						case 7 -> in.skipBytes(12);
						case 8 -> in.skipBytes(8);
						case 9 -> { if (in.readBoolean()) in.skipBytes(8); }
						case 11 -> { if (in.readBoolean()) in.skipBytes(16); }
						case 13 -> nbt(false);
						case 14 -> particle();
						case 15 -> { varInt(); varInt(); varInt(); }
						default -> throw new IllegalArgumentException("Metadata type");
					}
				}
			}
		}

		void particle() {
			int id = varInt();
			if (id == (protocol == 498 ? 3 : 4) || id == (protocol == 498 ? 23 : 25)) {
				varInt();
			} else if (id == (protocol == 498 ? 14 : 15)) {
				in.skipBytes(16);
			} else if (id == (protocol == 498 ? 32 : 36)) {
				item();
			} else if (protocol >= 755 && id == 16) {
				in.skipBytes(28);
			} else if (protocol >= 755 && id == 37) {
				in.skipBytes(8);
				switch (string()) {
					case "minecraft:block" -> in.skipBytes(8);
					case "minecraft:entity" -> varInt();
					default -> throw new IllegalArgumentException("Vibration destination");
				}
				varInt();
			}
		}

		void skipString() {
			in.skipBytes(varInt());
		}

		String string() {
			return in.readCharSequence(varInt(), StandardCharsets.UTF_8).toString();
		}

		int varInt() {
			int value = 0;
			for (int i = 0; i < 5; ++i) {
				int b = in.readUnsignedByte();
				if (i == 4 && (b & 0xF0) != 0) throw new IllegalArgumentException("VarInt overflow");
				value |= (b & 0x7F) << (i * 7);
				if ((b & 0x80) == 0) return value;
			}
			throw new IllegalArgumentException("VarInt overflow");
		}
	}
}
