package net.lax1dude.eaglercraft.backend.server.base.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

/** Hides head equipment with oversized NBT names from browser clients. */
public final class EquipmentNBTGuard extends ChannelOutboundHandlerAdapter {

	// Defensive policy, not a vanilla format limit. Normal item keys are much shorter.
	static final int MAX_NAME_BYTES = 1024;
	private final int protocol;
	private final Runnable reportMitigation;
	private boolean reported;

	public EquipmentNBTGuard(int protocol, Runnable reportMitigation) {
		this.protocol = protocol;
		this.reportMitigation = reportMitigation;
	}

	public static boolean supports(int protocol) {
		return packetId(protocol) >= 0;
	}

	private static int packetId(int protocol) {
		// Clientbound PLAY entity_equipment; these versions have explicitly verified layouts.
		return switch (protocol) {
			case 47 -> 0x04;
			case 340 -> 0x3F;
			case 498 -> 0x46;
			case 755, 756 -> 0x50;
			default -> -1;
		};
	}

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		if (msg instanceof ByteBuf original) {
			ByteBuf replacement = sanitize(original, protocol);
			if (replacement != original) {
				original.release();
				msg = replacement;
				if (!reported) {
					reported = true;
					reportMitigation.run();
				}
			}
		}
		ctx.write(msg, promise);
	}

	// Returns the original without retaining/moving it, or a new caller-owned buffer.
	static ByteBuf sanitize(ByteBuf original, int protocol) {
		if (!supports(protocol)) return original;
		ByteBuf in = original.duplicate();
		int[] starts;
		int[] ends;
		int replacements = 0;
		try {
			if (varInt(in) != packetId(protocol)) return original;
			starts = new int[6];
			ends = new int[6];
			varInt(in); // Entity ID
			int entries = 0;
			boolean more;
			do {
				if (++entries > 6) return original;
				int slot = protocol == 47 ? in.readShort() : protocol >= 755 ? in.readUnsignedByte() : varInt(in);
				more = protocol >= 755 && (slot & 0x80) != 0;
				int slotId = more ? slot & 0x7F : slot;
				if (slotId < 0 || slotId > (protocol == 47 ? 4 : 5)) return original;
				int itemStart = in.readerIndex();
				if (protocol <= 340) {
					if (in.readShort() == -1) continue;
					in.skipBytes(3); // Count and legacy damage
				} else {
					int present = in.readUnsignedByte();
					if (present == 0) continue;
					if (present != 1) return original;
					varInt(in);
					in.skipBytes(1); // Count
				}
				int type = in.readUnsignedByte();
				if (type == 0) continue;
				if (type != 10) return original;
				Scanner scanner = new Scanner(in);
				scanner.name();
				scanner.value(type, 0);
				if (scanner.oversizedName && slotId == (protocol == 47 ? 4 : 5)) {
					starts[replacements] = itemStart;
					ends[replacements++] = in.readerIndex();
				}
			} while (more);
			if (in.isReadable() || replacements == 0) return original;
		} catch (IndexOutOfBoundsException | IllegalArgumentException ex) {
			// This is not a general packet validator. Never partially rewrite an unrecognized packet.
			return original;
		}
		int size = original.readableBytes();
		int emptyItemBytes = protocol <= 340 ? 2 : 1;
		for (int i = 0; i < replacements; ++i) size -= ends[i] - starts[i] - emptyItemBytes;
		ByteBuf out = original.alloc().buffer(size);
		int cursor = original.readerIndex();
		for (int i = 0; i < replacements; ++i) {
			out.writeBytes(original, cursor, starts[i] - cursor);
			// Send an explicit empty slot so an earlier head item does not remain visible.
			if (protocol <= 340) out.writeShort(-1);
			else out.writeByte(0);
			cursor = ends[i];
		}
		out.writeBytes(original, cursor, original.writerIndex() - cursor);
		return out;
	}

	private static int varInt(ByteBuf in) {
		int value = 0;
		for (int i = 0; i < 5; ++i) {
			int b = in.readUnsignedByte();
			if (i == 4 && (b & 0xF0) != 0) throw new IllegalArgumentException("VarInt overflow");
			value |= (b & 0x7F) << (i * 7);
			if ((b & 0x80) == 0) return value;
		}
		throw new IllegalArgumentException("VarInt overflow");
	}

	private static final class Scanner {
		private final ByteBuf in;
		private int budget = 65536;
		private boolean oversizedName;

		Scanner(ByteBuf in) { this.in = in; }

		void name() {
			int length = in.readUnsignedShort();
			oversizedName |= length > MAX_NAME_BYTES;
			in.skipBytes(length);
		}

		void value(int type, int depth) {
			if (depth > 64 || --budget < 0) throw new IllegalArgumentException("NBT scan limit");
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
						throw new IllegalArgumentException("NBT list length/type");
					}
					for (int i = 0; i < count; ++i) value(child, depth + 1);
				}
				case 10 -> {
					int child;
					while ((child = in.readUnsignedByte()) != 0) {
						name();
						value(child, depth + 1);
					}
				}
				default -> throw new IllegalArgumentException("NBT type");
			}
		}
	}
}
