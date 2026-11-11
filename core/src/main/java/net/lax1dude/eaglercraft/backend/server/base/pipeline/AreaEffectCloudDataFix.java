package net.lax1dude.eaglercraft.backend.server.base.pipeline;

import java.util.HashSet;
import java.util.Set;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

/**
 * Renumbers area effect cloud entity data for Eaglercraft 1.12.2 clients. That client declares
 * the cloud's fields as indexes 8-13 instead of the protocol's 6-11, so unconverted data puts a
 * Boolean in the radius and the client's particle loop never ends, freezing the tab.
 */
public final class AreaEffectCloudDataFix extends ChannelOutboundHandlerAdapter {

	private static final int ADD_ENTITY = 0x00;
	private static final int LOGIN = 0x23;
	private static final int REMOVE_ENTITIES = 0x32;
	private static final int RESPAWN = 0x35;
	private static final int SET_ENTITY_DATA = 0x3C;
	private static final int CLOUD_OBJECT_TYPE = 3;
	private static final int FIRST_CLOUD_INDEX = 6;
	private static final int LAST_CLOUD_INDEX = 11;
	private static final int INDEX_SHIFT = 2;

	private final Set<Integer> clouds = new HashSet<>();

	public static boolean supports(int protocol) {
		return protocol == 340;
	}

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		if (msg instanceof ByteBuf original) {
			ByteBuf replacement = handle(original);
			if (replacement != original) {
				original.release();
				msg = replacement;
			}
		}
		ctx.write(msg, promise);
	}

	private ByteBuf handle(ByteBuf original) {
		ByteBuf in = original.duplicate();
		try {
			switch (varInt(in)) {
				case ADD_ENTITY -> {
					int entityId = varInt(in);
					in.skipBytes(16); // UUID
					if (in.readByte() == CLOUD_OBJECT_TYPE) clouds.add(entityId);
				}
				case REMOVE_ENTITIES -> {
					int count = varInt(in);
					for (int i = 0; i < count; ++i) clouds.remove(varInt(in));
				}
				case LOGIN, RESPAWN -> clouds.clear();
				case SET_ENTITY_DATA -> {
					if (clouds.contains(varInt(in))) return renumber(original, in);
				}
				default -> {}
			}
		} catch (IndexOutOfBoundsException | IllegalArgumentException ex) {
			// Leave malformed or unknown layouts to the normal protocol handler.
		}
		return original;
	}

	private static ByteBuf renumber(ByteBuf original, ByteBuf in) {
		int[] positions = new int[LAST_CLOUD_INDEX - FIRST_CLOUD_INDEX + 1];
		int count = 0;
		int index;
		while ((index = in.readUnsignedByte()) != 0xFF) {
			if (index >= FIRST_CLOUD_INDEX && index <= LAST_CLOUD_INDEX) {
				if (count == positions.length) throw new IllegalArgumentException("Duplicate entity data index");
				positions[count++] = in.readerIndex() - 1;
			}
			skipValue(in, varInt(in));
		}
		if (in.isReadable() || count == 0) return original;
		ByteBuf out = original.copy();
		int base = original.readerIndex();
		for (int i = 0; i < count; ++i) {
			int pos = positions[i] - base;
			out.setByte(pos, out.getUnsignedByte(pos) + INDEX_SHIFT);
		}
		return out;
	}

	private static void skipValue(ByteBuf in, int type) {
		switch (type) {
			case 0, 6 -> in.skipBytes(1); // Byte, Boolean
			case 1, 10, 12 -> varInt(in); // VarInt, Direction, Optional block state
			case 2 -> in.skipBytes(4); // Float
			case 3, 4 -> in.skipBytes(varInt(in)); // String, Chat
			case 7 -> in.skipBytes(12); // Rotation
			case 8 -> in.skipBytes(8); // Position
			case 9 -> { if (in.readBoolean()) in.skipBytes(8); } // Optional position
			case 11 -> { if (in.readBoolean()) in.skipBytes(16); } // Optional UUID
			default -> throw new IllegalArgumentException("Entity data type"); // Slot and NBT never appear on clouds
		}
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
}
