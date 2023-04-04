package redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import redis.netty4.Command;

import java.io.IOException;
import java.util.List;

import static redis.netty4.RedisReplyDecoder.readLong;

/**
 * Decode commands.
 */
public class RedisCommandDecoder extends ReplayingDecoder<RedisCommandDecoder.Pointer> {

    public static class Pointer {
        private byte[][] bytes;
        private int idx;
    }

    @Override
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        Pointer pointer = state();
        if (pointer != null) {
            byte[][] bytes = pointer.bytes;
            int numArgs = bytes.length;
            for (int i = pointer.idx; i < numArgs; i++) {
                if (in.readByte() == '$') {
                    long l = readLong(in);
                    if (l > Integer.MAX_VALUE) {
                        throw new IllegalArgumentException("Java only supports arrays up to " + Integer.MAX_VALUE + " in size");
                    }
                    int size = (int) l;
                    bytes[i] = new byte[size];
                    in.readBytes(bytes[i]);
                    if (in.bytesBefore((byte) '\r') != 0) {
                        throw new Exception("Argument doesn't end in CRLF");
                    }
                    in.skipBytes(2);
                    pointer.idx = pointer.idx + 1;
                    state(pointer);
                    checkpoint();
                } else {
                    throw new IOException("Unexpected character");
                }
            }
            out.add(new Command(bytes));
            state(null);
        } else {
            if (in.readByte() == '*') {
                long l = readLong(in);
                if (l > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("Java only supports arrays up to " + Integer.MAX_VALUE + " in size");
                }
                int numArgs = (int) l;
                if (numArgs < 0) {
                    throw new Exception("Invalid size: " + numArgs);
                }
                Pointer p = new Pointer();
                p.bytes = new byte[numArgs][];
                state(p);
                checkpoint();
            } else {
                throw new IOException("Unexpected character");
            }
        }
    }

}
